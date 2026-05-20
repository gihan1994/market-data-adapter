package com.example.marketdata.health;

import com.example.marketdata.leader.LeaderElectionService;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

/**
 * Reports the pod's leadership state. NOT used for readiness — a standby pod is
 * still "ready" to serve (it can become leader at any moment).
 */
@Component("leader")
public class LeaderHealthIndicator implements HealthIndicator {

    private final LeaderElectionService leader;

    public LeaderHealthIndicator(LeaderElectionService leader) {
        this.leader = leader;
    }

    @Override
    public Health health() {
        return Health.up()
                .withDetail("pod", leader.getPodName())
                .withDetail("role", leader.getRole().name())
                .withDetail("state", leader.getState().name())
                .withDetail("isLeader", leader.isLeader())
                .build();
    }
}
