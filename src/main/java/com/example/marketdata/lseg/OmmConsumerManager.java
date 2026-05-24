package com.example.marketdata.lseg;

import com.example.marketdata.config.MarketDataProperties;
import com.example.marketdata.config.MarketDataProperties.Lseg.ConnectionMode;
import com.example.marketdata.leader.PodRole;
import com.refinitiv.ema.access.EmaFactory;
import com.refinitiv.ema.access.OmmConsumer;
import com.refinitiv.ema.access.OmmConsumerConfig;
import com.refinitiv.ema.access.ReqMsg;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Owns the singleton {@link OmmConsumer} for this pod.
 *
 * <h2>Lifecycle</h2>
 * <ul>
 *   <li>{@code WARM_ELIGIBLE} pods open the consumer at startup (= 1 LSEG session)</li>
 *   <li>{@code COLD_ONLY} pods open lazily only if promoted to leader</li>
 *   <li>Only the active leader calls {@link #subscribe(String)} for each RIC</li>
 *   <li>A warm standby pod has a session but no subscriptions</li>
 * </ul>
 *
 * <h2>Authentication modes</h2>
 * <ul>
 *   <li>{@code ON_PREM_TREP} — DACS auth: username + position (egress IP) + applicationId</li>
 *   <li>{@code RTO_CLOUD} — OAuth2 V2: clientId + clientSecret</li>
 * </ul>
 *
 * <h2>Cost model (per-DACS-user licensing)</h2>
 * Each {@code OmmConsumer.initialize()} opens its own TCP connection + LOGIN to ADS.
 * All pods MUST use the same DACS username so that all sessions count against the same
 * named user license. Across the 4 pods we keep exactly 2 concurrent sessions open
 * (1 active leader + 1 cross-hall warm standby), well under the DACS user's MaxLogins.
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
        log.info("Pod role={} hall={} connectionMode={} adsHost={}",
                role, props.getPod().getHall(),
                props.getLseg().getConnectionMode(), props.getLseg().getAdsHost());
        if (role == PodRole.WARM_ELIGIBLE) {
            log.info("WARM_ELIGIBLE — opening EMA session at startup (this counts as 1 DACS login)");
            ensureConsumer();
        } else {
            log.info("COLD_ONLY — no EMA session at startup (0 DACS logins)");
        }
    }

    /**
     * Open the EMA consumer if not already open. Thread-safe (idempotent).
     * Each successful call opens one TCP+LOGIN to ADS, counting as one DACS session.
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
                    .consumerName(props.getLseg().getConsumerName());

            ConnectionMode mode = props.getLseg().getConnectionMode();
            if (mode == ConnectionMode.ON_PREM_TREP) {
                configureForOnPremTrep(cfg);
            } else {
                configureForRtoCloud(cfg);
            }

            consumer = EmaFactory.createOmmConsumer(cfg, callback);
            initialized.set(true);
            log.info("OmmConsumer initialized ({} mode) — 1 LSEG session active", mode);
        } catch (Throwable t) {
            log.error("Failed to initialize OmmConsumer", t);
            throw new IllegalStateException("OmmConsumer init failed", t);
        }
    }

    /**
     * On-prem TREP / RTDS via RSSL_SOCKET + DACS authentication.
     * Channel host/port comes from EmaConfig.xml (with our hall-specific overrides),
     * username + position + applicationId go into the LOGIN admin message.
     */
    private void configureForOnPremTrep(OmmConsumerConfig cfg) {
        String username = props.getLseg().getDacsUsername();
        if (!StringUtils.hasText(username)) {
            throw new IllegalStateException("marketdata.lseg.dacs-username is required for ON_PREM_TREP");
        }

        cfg.username(username);

        // Override host/port from properties — useful when EmaConfig.xml has placeholder values.
        String host = props.getLseg().getAdsHost();
        if (StringUtils.hasText(host)) {
            cfg.host(host + ":" + props.getLseg().getAdsPort());
        }

        // Position + applicationId are set via a custom LOGIN ReqMsg admin message.
        // EMA also supports OmmConsumerConfig.position()/applicationId() in newer versions; use whichever is available.
        String position = props.getLseg().getDacsPosition();
        if (StringUtils.hasText(position)) {
            try {
                // Reflective call so the code compiles against older EMA versions too.
                cfg.getClass().getMethod("position", String.class).invoke(cfg, position);
            } catch (Exception ignored) {
                log.debug("OmmConsumerConfig.position() not available — will set via Login admin message");
            }
        }
        String applicationId = props.getLseg().getDacsApplicationId();
        if (StringUtils.hasText(applicationId)) {
            try {
                cfg.getClass().getMethod("applicationId", String.class).invoke(cfg, applicationId);
            } catch (Exception ignored) {
                log.debug("OmmConsumerConfig.applicationId() not available — will set via Login admin message");
            }
        }

        log.info("DACS login: username={} position={} applicationId={} host={}",
                username, position, applicationId, host);
    }

    /**
     * Refinitiv Real-Time Optimized (cloud) via RSSL_ENCRYPTED + OAuth2 V2 client credentials.
     */
    private void configureForRtoCloud(OmmConsumerConfig cfg) {
        cfg.clientId(props.getLseg().getClientId())
           .clientSecret(props.getLseg().getClientSecret());
        log.info("RTO login: clientId={}", props.getLseg().getClientId());
    }

    /**
     * Send a Request message for a RIC. Returns the handle (or a mock id in mock mode).
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
