package com.example.marketdata.subscription;

import com.example.marketdata.config.MarketDataProperties;
import com.example.marketdata.domain.RicRegistryEntity;
import com.example.marketdata.leader.DemotedEvent;
import com.example.marketdata.leader.LeaderElectionService;
import com.example.marketdata.leader.PromotedToLeaderEvent;
import com.example.marketdata.lseg.MarketDataTick;
import com.example.marketdata.lseg.OmmConsumerManager;
import com.example.marketdata.lseg.TickListener;
import com.example.marketdata.publisher.MarketDataKafkaProducer;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RedissonClient;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Executors;
import java.math.BigDecimal;

/**
 * Orchestrates RIC subscriptions on top of the EMA consumer.
 *
 * Responsibilities:
 *  - Consume SUBSCRIBE / UNSUBSCRIBE requests (called from {@link SubscriptionRequestListener})
 *  - Refcount-driven: only call EMA subscribe on first subscriber, unsubscribe on last
 *  - On promotion to leader: resubscribe all active RICs from the registry
 *  - On demotion: stop publishing ticks (but keep subscriptions open if warm)
 *  - Cache latest tick + last-published timestamp in Redis
 *  - Forward live ticks to the Kafka producer
 */
@Service
@Slf4j
public class SubscriptionManager implements TickListener {

    private final OmmConsumerManager omm;
    private final RicRegistryService registry;
    private final RefcountService refcount;
    private final MarketDataKafkaProducer producer;
    private final LeaderElectionService leader;
    private final MarketDataProperties props;
    private final RedissonClient redisson;
    private final com.example.marketdata.lseg.MarketDataCallbackHandler callback;

    private final ScheduledExecutorService drainScheduler = Executors.newScheduledThreadPool(2,
            r -> { Thread t = new Thread(r, "drain-"); t.setDaemon(true); return t; });
    private final Map<String, ScheduledFuture<?>> pendingDrains = new ConcurrentHashMap<>();
    private volatile boolean publishing = false;

    public SubscriptionManager(OmmConsumerManager omm,
                               RicRegistryService registry,
                               RefcountService refcount,
                               MarketDataKafkaProducer producer,
                               LeaderElectionService leader,
                               MarketDataProperties props,
                               RedissonClient redisson,
                               com.example.marketdata.lseg.MarketDataCallbackHandler callback) {
        this.omm = omm;
        this.registry = registry;
        this.refcount = refcount;
        this.producer = producer;
        this.leader = leader;
        this.props = props;
        this.redisson = redisson;
        this.callback = callback;
        callback.addListener(this);
    }

    // ============================================================
    //  Request handling — invoked by SubscriptionRequestListener
    // ============================================================

    public void handleSubscribe(SubscriptionRequest req) {
        if (!leader.isLeader()) {
            log.debug("Not leader — ignoring subscribe (will be processed by current leader)");
            return;
        }
        boolean firstSubscriber = registry.recordSubscribe(req.getRic(), req.getRequester());
        long count = refcount.increment(req.getRic());

        // If we had scheduled a drain for this RIC, cancel it
        ScheduledFuture<?> pending = pendingDrains.remove(req.getRic());
        if (pending != null) {
            pending.cancel(false);
            log.info("Cancelled pending drain for {} (new subscriber arrived)", req.getRic());
        }

        if (firstSubscriber || count == 1) {
            registry.updateState(req.getRic(), SubscriptionState.SUBSCRIBING);
            try {
                omm.subscribe(req.getRic());
            } catch (Exception e) {
                log.error("EMA subscribe failed for {}: {}", req.getRic(), e.getMessage());
                registry.updateState(req.getRic(), SubscriptionState.ERROR);
            }
        } else {
            // Already subscribed — publish cached snapshot to this requester via Kafka
            publishCachedSnapshot(req.getRic());
        }
    }

    public void handleUnsubscribe(SubscriptionRequest req) {
        if (!leader.isLeader()) {
            log.debug("Not leader — ignoring unsubscribe");
            return;
        }
        boolean wasLast = registry.recordUnsubscribe(req.getRic(), req.getRequester());
        long count = refcount.decrement(req.getRic());

        if (wasLast || count == 0) {
            registry.updateState(req.getRic(), SubscriptionState.DRAINING);
            scheduleDrain(req.getRic());
        }
    }

    private void scheduleDrain(String ric) {
        int grace = props.getSubscription().getDrainGraceSeconds();
        log.info("Scheduling drain for ric={} in {}s", ric, grace);
        ScheduledFuture<?> future = drainScheduler.schedule(() -> {
            if (refcount.get(ric) > 0) {
                log.info("Drain aborted for {} — refcount went positive again", ric);
                return;
            }
            log.info("Drain complete for {} — closing EMA stream", ric);
            omm.unsubscribe(ric);
            registry.deactivateRic(ric);
            refcount.reset(ric);
            pendingDrains.remove(ric);
        }, grace, TimeUnit.SECONDS);
        pendingDrains.put(ric, future);
    }

    private void publishCachedSnapshot(String ric) {
        try {
            var bucket = redisson.<String>getBucket(props.getSubscription().getPriceKeyPrefix() + ric);
            String cached = bucket.get();
            if (cached != null) {
                log.debug("Publishing cached snapshot for {}", ric);
                producer.publishCachedSnapshot(ric, cached);
            }
        } catch (Exception e) {
            log.warn("Cached snapshot publish failed for {}: {}", ric, e.getMessage());
        }
    }

    // ============================================================
    //  Leader transitions
    // ============================================================

    @EventListener
    @Async("resubscribeExecutor")
    public void onPromoted(PromotedToLeaderEvent event) {
        log.info("Promoted to leader — resubscribing all active RICs");
        publishing = true;
        omm.ensureConsumer();   // make sure session is open (no-op if warm-eligible)

        List<RicRegistryEntity> active = registry.findActiveRics();
        log.info("Resubscribing {} active RICs", active.size());

        Set<String> alreadySubscribed = new HashSet<>(omm.currentSubscriptions().keySet());
        for (RicRegistryEntity r : active) {
            if (alreadySubscribed.contains(r.getRic())) continue;
            try {
                omm.subscribe(r.getRic());
                registry.updateState(r.getRic(), SubscriptionState.SUBSCRIBING);
            } catch (Exception e) {
                log.error("Failed to resubscribe {}: {}", r.getRic(), e.getMessage());
                registry.updateState(r.getRic(), SubscriptionState.ERROR);
            }
        }
        log.info("Resubscribe complete");
    }

    @EventListener
    public void onDemoted(DemotedEvent event) {
        log.info("Demoted from leader ({}) — stopping tick publication; keeping warm session if applicable",
                event.getReason());
        publishing = false;
        // Cancel any pending drains — the new leader will manage them
        pendingDrains.values().forEach(f -> f.cancel(false));
        pendingDrains.clear();
    }

    // ============================================================
    //  Tick callback (from MarketDataCallbackHandler)
    // ============================================================

    @Override
    public void onTick(MarketDataTick tick) {
        if (!publishing && !leader.isLeader()) {
            // Warm standby receives ticks for snapshots only — discard
            return;
        }
        try {
            cacheTick(tick);
            producer.publishTick(tick);
            if (tick.getType() == MarketDataTick.Type.SNAPSHOT && tick.isComplete()) {
                registry.updateState(tick.getRic(), SubscriptionState.ACTIVE);
            }
        } catch (Exception e) {
            log.error("Failed to handle tick for {}", tick.getRic(), e);
        }
    }

    private void cacheTick(MarketDataTick tick) {
        try {
            var priceKey = props.getSubscription().getPriceKeyPrefix() + tick.getRic();
            var lastPubKey = props.getSubscription().getLastPublishedKeyPrefix() + tick.getRic();
            var ttl = Duration.ofSeconds(props.getSubscription().getPriceTtlSeconds());

            String payload = String.format("{\"bid\":%s,\"ask\":%s,\"last\":%s,\"ts\":%d}",
                    safe(tick.getBid()), safe(tick.getAsk()), safe(tick.getLast()),
                    tick.getReceivedAt() != null ? tick.getReceivedAt().toEpochMilli() : System.currentTimeMillis());

            redisson.getBucket(priceKey).set(payload, ttl);
            redisson.getBucket(lastPubKey).set(String.valueOf(System.currentTimeMillis()), ttl);
        } catch (Exception e) {
            log.debug("Tick cache failed for {}: {}", tick.getRic(), e.getMessage());
        }
    }

    private static String safe(BigDecimal v) { return v == null ? "null" : v.toPlainString(); }
}
