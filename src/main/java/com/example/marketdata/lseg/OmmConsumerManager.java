package com.example.marketdata.lseg;

import com.example.marketdata.config.MarketDataProperties;
import com.example.marketdata.leader.PodRole;
import com.refinitiv.ema.access.EmaFactory;
import com.refinitiv.ema.access.OmmConsumer;
import com.refinitiv.ema.access.OmmConsumerConfig;
import com.refinitiv.ema.access.ReqMsg;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Owns the singleton {@link OmmConsumer} for this pod.
 *
 * Lifecycle:
 *   - For WARM_ELIGIBLE pods: consumer is opened at startup (= 1 LSEG login).
 *     This is the "warm standby session" — logged in but not subscribed.
 *   - For COLD_ONLY pods: consumer is NOT opened at startup. It opens lazily
 *     only if the pod is promoted to leader.
 *   - The active leader sends {@link #subscribe(String)} for each RIC in the registry.
 *   - A warm standby sends no subscribe calls until promoted.
 *
 * One pod = one EMA login charge.
 */
@Service
@Slf4j
public class OmmConsumerManager {

    private final MarketDataProperties props;
    private final MarketDataCallbackHandler callback;

    private final Map<String, Long> handlesByRic = new ConcurrentHashMap<>();
    private final AtomicBoolean initialized = new AtomicBoolean(false);

    private volatile OmmConsumer consumer;

    public OmmConsumerManager(MarketDataProperties props, MarketDataCallbackHandler callback) {
        this.props = props;
        this.callback = callback;
    }

    @PostConstruct
    void init() {
        PodRole role = PodRole.fromString(props.getPod().getRole());
        if (role == PodRole.WARM_ELIGIBLE) {
            log.info("Pod role WARM_ELIGIBLE — opening EMA session at startup (warm login)");
            ensureConsumer();
        } else {
            log.info("Pod role COLD_ONLY — EMA session NOT opened at startup");
        }
    }

    /**
     * Open the EMA consumer if not already open. Thread-safe (idempotent).
     * Each call to {@link OmmConsumer#initialize()} (well, {@link EmaFactory#createOmmConsumer})
     * counts as one LSEG login.
     */
    public synchronized void ensureConsumer() {
        if (initialized.get()) return;
        if (props.getLseg().isMockMode()) {
            log.warn("LSEG mock mode — skipping real OmmConsumer creation");
            initialized.set(true);
            return;
        }

        try {
            OmmConsumerConfig cfg = EmaFactory.createOmmConsumerConfig()
                    .consumerName(props.getLseg().getConsumerName())
                    .clientId(props.getLseg().getClientId())
                    .clientSecret(props.getLseg().getClientSecret());

            consumer = EmaFactory.createOmmConsumer(cfg, callback);
            initialized.set(true);
            log.info("OmmConsumer initialized — 1 LSEG login active");
        } catch (Throwable t) {
            log.error("Failed to initialize OmmConsumer", t);
            throw new IllegalStateException("OmmConsumer init failed", t);
        }
    }

    /**
     * Send a Request message for a RIC. Returns the handle (or 0 in mock mode).
     */
    public long subscribe(String ric) {
        ensureConsumer();
        if (props.getLseg().isMockMode()) {
            long handle = System.nanoTime();
            handlesByRic.put(ric, handle);
            log.debug("[mock] subscribed {}", ric);
            return handle;
        }
        if (handlesByRic.containsKey(ric)) {
            log.debug("Already subscribed: {}", ric);
            return handlesByRic.get(ric);
        }
        ReqMsg req = EmaFactory.createReqMsg()
                .serviceName(props.getLseg().getServiceName())
                .name(ric);
        long handle = consumer.registerClient(req, callback);
        handlesByRic.put(ric, handle);
        log.info("Subscribed RIC={} handle={}", ric, handle);
        return handle;
    }

    /** Close a single RIC stream. */
    public void unsubscribe(String ric) {
        Long handle = handlesByRic.remove(ric);
        if (handle == null) return;
        if (props.getLseg().isMockMode()) {
            log.debug("[mock] unsubscribed {}", ric);
            return;
        }
        try {
            consumer.unregister(handle);
            log.info("Unsubscribed RIC={}", ric);
        } catch (Exception e) {
            log.warn("Failed to unsubscribe RIC={}: {}", ric, e.getMessage());
        }
    }

    /** Returns the active RIC handle set (snapshot copy). */
    public Map<String, Long> currentSubscriptions() {
        return Map.copyOf(handlesByRic);
    }

    public boolean isConnected() {
        return initialized.get() && (props.getLseg().isMockMode() || consumer != null);
    }

    @PreDestroy
    void shutdown() {
        if (consumer != null) {
            try {
                log.info("Closing OmmConsumer ({} active subscriptions)", handlesByRic.size());
                consumer.uninitialize();
            } catch (Exception e) {
                log.warn("OmmConsumer uninitialize failed: {}", e.getMessage());
            }
        }
        handlesByRic.clear();
        initialized.set(false);
    }
}
