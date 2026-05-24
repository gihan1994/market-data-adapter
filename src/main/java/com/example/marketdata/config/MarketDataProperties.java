package com.example.marketdata.config;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Strongly-typed config for the marketdata.* properties tree.
 *
 * <p>The service runs as 4 pods spread across two OpenShift clusters (hall1 + hall2)
 * sharing one Enterprise Redis cluster for cross-hall leader election. Each hall has a
 * static egress IP whitelisted on the TREP server.
 */
@ConfigurationProperties(prefix = "marketdata")
@Validated
@Getter
@Setter
public class MarketDataProperties {

    @NotNull private Pod pod = new Pod();
    @NotNull private Leader leader = new Leader();
    @NotNull private Lseg lseg = new Lseg();
    @NotNull private Subscription subscription = new Subscription();
    @NotNull private Kafka kafka = new Kafka();
    @NotNull private Recovery recovery = new Recovery();

    @Getter @Setter
    public static class Pod {
        /** OpenShift cluster identifier — "hall1" or "hall2". Used for metrics + audit tagging. */
        @NotBlank private String hall = "hall1";
        /** "warm-eligible" or "cold-only" — see PodRole enum */
        @NotBlank private String role = "warm-eligible";
        /** StatefulSet pod name (e.g. market-data-service-0) */
        @NotBlank private String name = "local-pod";
    }

    @Getter @Setter
    public static class Leader {
        /**
         * Lock key is shared across BOTH halls — Enterprise Redis enables one global leader
         * across the two clusters. Do not include a hall suffix.
         */
        @NotBlank private String lockKey = "marketdata:leader:lock";
        @Positive private int lockTtlSeconds = 5;
        @Positive private int heartbeatSeconds = 2;
        @Positive private int acquireRetrySeconds = 1;
    }

    @Getter @Setter
    public static class Lseg {

        public enum ConnectionMode {
            /** On-prem TREP / RTDS via RSSL_SOCKET + DACS auth. */
            ON_PREM_TREP,
            /** Refinitiv Real-Time Optimized (cloud) via RSSL_ENCRYPTED + OAuth2 V2. */
            RTO_CLOUD
        }

        @NotNull private ConnectionMode connectionMode = ConnectionMode.ON_PREM_TREP;

        // ----- On-prem TREP (DACS auth) -----
        /** DACS username — same for ALL pods (per-user licensing). */
        private String dacsUsername = "";
        /** Position string sent in LOGIN request. Defaults to the static hall egress IP. */
        private String dacsPosition = "";
        /** Application identifier sent in LOGIN attribute. */
        private String dacsApplicationId = "256";
        /** Primary ADS endpoint for this hall (host:port). */
        private String adsHost = "";
        private int adsPort = 14002;
        /** Backup ADS endpoint in the same hall (for ChannelSet failover). */
        private String adsBackupHost = "";
        private int adsBackupPort = 14002;
        /** Service name on ADS to subscribe through (e.g. ELEKTRON_DD, DIRECT_FEED). */
        @NotBlank private String serviceName = "ELEKTRON_DD";

        // ----- RTO cloud (OAuth2 V2) — optional, only used when connectionMode=RTO_CLOUD -----
        private String clientId = "";
        private String clientSecret = "";
        private String tokenUrl = "https://api.refinitiv.com/auth/oauth2/v2/token";
        private String scope = "trapi";

        // ----- Common -----
        private String emaConfigFile = "classpath:EmaConfig.xml";
        @NotBlank private String consumerName = "Consumer_1";
        /** Skip real LSEG connection — for local dev / unit tests. */
        private boolean mockMode = false;
    }

    @Getter @Setter
    public static class Subscription {
        @Positive private int drainGraceSeconds = 30;
        @Positive private int batchResubscribeSize = 100;
        @NotBlank private String refcountKeyPrefix = "marketdata:subscription:refcount:";
        @NotBlank private String activeRicsKey = "marketdata:ric:active";
        @NotBlank private String priceKeyPrefix = "marketdata:ric:price:";
        @NotBlank private String lastPublishedKeyPrefix = "marketdata:ric:last-published:";
        @Positive private int priceTtlSeconds = 60;
    }

    @Getter @Setter
    public static class Kafka {
        @NotBlank private String topicRequests = "market-data-requests";
        @NotBlank private String topicUpdates = "market-data-updates";
        @NotBlank private String topicControl = "market-data-control";
    }

    @Getter @Setter
    public static class Recovery {
        @Positive private long gapThresholdMillis = 2000;
        private boolean snapshotOnRecovery = true;
    }
}
