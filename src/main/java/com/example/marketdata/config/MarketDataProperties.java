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
        /** "warm-eligible" or "cold-only" — see PodRole enum */
        @NotBlank private String role = "warm-eligible";
        @NotBlank private String name = "local-pod";
    }

    @Getter @Setter
    public static class Leader {
        @NotBlank private String lockKey = "marketdata:leader:lock";
        @Positive private int lockTtlSeconds = 5;
        @Positive private int heartbeatSeconds = 2;
        @Positive private int acquireRetrySeconds = 1;
    }

    @Getter @Setter
    public static class Lseg {
        private String clientId = "";
        private String clientSecret = "";
        private String tokenUrl = "https://api.refinitiv.com/auth/oauth2/v2/token";
        private String scope = "trapi";
        private String emaConfigFile = "classpath:EmaConfig.xml";
        @NotBlank private String consumerName = "Consumer_1";
        @NotBlank private String serviceName = "ELEKTRON_DD";
        /** Skip real LSEG connection — for local dev / tests. */
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
