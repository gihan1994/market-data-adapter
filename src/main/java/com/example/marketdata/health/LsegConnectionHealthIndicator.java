package com.example.marketdata.health;

import com.example.marketdata.leader.LeaderElectionService;
import com.example.marketdata.leader.PodRole;
import com.example.marketdata.lseg.OmmConsumerManager;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

/**
 * Custom Spring Actuator health indicator covering:
 *   - LSEG EMA connection status (must be UP for warm-eligible pods)
 *   - Leader role + state
 *
 * Used by OpenShift readiness probe: a pod is "ready" only if it has the connection
 * it's supposed to have for its role.
 */
@Component("lseg")
public class LsegConnectionHealthIndicator implements HealthIndicator {

    private final OmmConsumerManager omm;
    private final LeaderElectionService leader;

    public LsegConnectionHealthIndicator(OmmConsumerManager omm, LeaderElectionService leader) {
        this.omm = omm;
        this.leader = leader;
    }

    @Override
    public Health health() {
        boolean connected = omm.isConnected();
        PodRole role = leader.getRole();
        boolean expectConnection = leader.isLeader() || role == PodRole.WARM_ELIGIBLE;

        Health.Builder b = (connected || !expectConnection) ? Health.up() : Health.down();
        return b
                .withDetail("emaConnected", connected)
                .withDetail("podRole", role.name())
                .withDetail("leaderState", leader.getState().name())
                .withDetail("activeSubscriptions", omm.currentSubscriptions().size())
                .build();
    }
}
