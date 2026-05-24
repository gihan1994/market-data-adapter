package com.example.marketdata.subscription;

import com.example.marketdata.config.MarketDataProperties;
import com.example.marketdata.domain.AuditEventType;
import com.example.marketdata.domain.RicRegistryEntity;
import com.example.marketdata.domain.SubscriptionAuditEntity;
import com.example.marketdata.domain.SubscriptionRequestEntity;
import com.example.marketdata.repository.RicRegistryRepository;
import com.example.marketdata.repository.SubscriptionAuditRepository;
import com.example.marketdata.repository.SubscriptionRequestRepository;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Set;

/**
 * Maintains both the durable RIC registry (PostgreSQL) and the live set of active
 * RICs (Redis sorted set keyed by first-subscribed timestamp).
 */
@Service
@Slf4j
public class RicRegistryService {

    private final RicRegistryRepository ricRepo;
    private final SubscriptionRequestRepository reqRepo;
    private final SubscriptionAuditRepository auditRepo;
    private final RedissonClient redisson;
    private final MarketDataProperties props;

    public RicRegistryService(RicRegistryRepository ricRepo,
                              SubscriptionRequestRepository reqRepo,
                              SubscriptionAuditRepository auditRepo,
                              RedissonClient redisson,
                              MarketDataProperties props) {
        this.ricRepo = ricRepo;
        this.reqRepo = reqRepo;
        this.auditRepo = auditRepo;
        this.redisson = redisson;
        this.props = props;
    }

    /**
     * Record a business MS's subscription. Returns true if this is the first active
     * subscriber for this RIC (i.e. caller should send a real EMA ReqMsg).
     */
    @Transactional
    public boolean recordSubscribe(String ric, String requester) {
        // 1. Ensure RIC row exists (or reactivate)
        RicRegistryEntity entity = ricRepo.findById(ric).orElseGet(() ->
                RicRegistryEntity.builder()
                        .ric(ric)
                        .serviceName(props.getLseg().getServiceName())
                        .active(true)
                        .firstSubscribed(OffsetDateTime.now())
                        .lastStateChange(OffsetDateTime.now())
                        .state(SubscriptionState.REQUESTED)
                        .build());

        boolean isNew = !entity.isActive();
        entity.setActive(true);
        if (entity.getState() == SubscriptionState.CLOSED || entity.getState() == SubscriptionState.DRAINING) {
            entity.setState(SubscriptionState.REQUESTED);
            entity.setLastStateChange(OffsetDateTime.now());
            isNew = true;
        }
        ricRepo.save(entity);

        // 2. Idempotent insert into subscription_requests
        SubscriptionRequestEntity req = reqRepo.findByBusinessMsAndRic(requester, ric)
                .orElseGet(() -> SubscriptionRequestEntity.builder()
                        .businessMs(requester).ric(ric).active(true).build());
        req.setActive(true);
        reqRepo.save(req);

        // 3. Live set in Redis
        RScoredSortedSet<String> active = redisson.getScoredSortedSet(props.getSubscription().getActiveRicsKey());
        active.add(System.currentTimeMillis(), ric);

        long active_subscribers = reqRepo.countByRicAndActiveTrue(ric);
        log.info("recordSubscribe ric={} requester={} totalActiveSubscribers={} firstSubscriber={}",
                ric, requester, active_subscribers, active_subscribers == 1);
        audit(AuditEventType.RIC_SUBSCRIBED, ric, "by=" + requester);
        return active_subscribers == 1;
    }

    /**
     * Record an unsubscribe. Returns true if this was the LAST active subscriber
     * (caller should drain & close the EMA stream).
     */
    @Transactional
    public boolean recordUnsubscribe(String ric, String requester) {
        reqRepo.deactivate(requester, ric);
        long remaining = reqRepo.countByRicAndActiveTrue(ric);
        log.info("recordUnsubscribe ric={} requester={} remaining={}", ric, requester, remaining);
        audit(AuditEventType.RIC_UNSUBSCRIBED, ric, "by=" + requester);
        return remaining == 0;
    }

    @Transactional
    public void deactivateRic(String ric) {
        ricRepo.deactivate(ric, OffsetDateTime.now());
        RScoredSortedSet<String> active = redisson.getScoredSortedSet(props.getSubscription().getActiveRicsKey());
        active.remove(ric);
        audit(AuditEventType.RIC_STATE_CHANGED, ric, "CLOSED");
    }

    @Transactional
    public void updateState(String ric, SubscriptionState newState) {
        ricRepo.updateState(ric, newState, OffsetDateTime.now());
        audit(AuditEventType.RIC_STATE_CHANGED, ric, newState.name());
    }

    @Transactional(readOnly = true)
    public List<RicRegistryEntity> findActiveRics() {
        return ricRepo.findAllByActiveTrue();
    }

    /** Active RICs in Redis (fast path; may diverge briefly from PG). */
    public Set<String> activeRicsFromRedis() {
        return new java.util.HashSet<>(
                redisson.<String>getScoredSortedSet(props.getSubscription().getActiveRicsKey()).readAll());
    }

    private void audit(AuditEventType type, String ric, String detail) {
        try {
            auditRepo.save(SubscriptionAuditEntity.builder()
                    .podName(props.getPod().getName())
                    .hall(props.getPod().getHall())
                    .ric(ric).eventType(type).detail(detail).build());
        } catch (Exception ignored) { /* don't fail flow on audit issue */ }
    }
}
