package com.example.marketdata.metrics;

import com.example.marketdata.leader.LeaderElectionService;
import com.example.marketdata.lseg.OmmConsumerManager;
import com.example.marketdata.subscription.RicRegistryService;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Component;

/**
 * Registers custom Micrometer gauges for the dashboard.
 *
 *   marketdata.subscriptions.active   = number of RICs subscribed via EMA
 *   marketdata.subscriptions.registry = number of active RICs in DB registry
 *   marketdata.leader                  = 1 if this pod is leader, else 0
 */
@Component
public class MarketDataMetrics {

    private final MeterRegistry meters;
    private final OmmConsumerManager omm;
    private final RicRegistryService registry;
    private final LeaderElectionService leader;

    public MarketDataMetrics(MeterRegistry meters,
                             OmmConsumerManager omm,
                             RicRegistryService registry,
                             LeaderElectionService leader) {
        this.meters = meters;
        this.omm = omm;
        this.registry = registry;
        this.leader = leader;
    }

    @PostConstruct
    void register() {
        Gauge.builder("marketdata.subscriptions.active",
                        () -> omm.currentSubscriptions().size())
                .description("Number of RICs subscribed via EMA (this pod)")
                .register(meters);

        Gauge.builder("marketdata.subscriptions.registry",
                        () -> registry.findActiveRics().size())
                .description("Number of active RICs in PostgreSQL registry")
                .register(meters);

        Gauge.builder("marketdata.leader",
                        () -> leader.isLeader() ? 1 : 0)
                .description("Whether this pod is the active leader (1/0)")
                .register(meters);
    }
}
