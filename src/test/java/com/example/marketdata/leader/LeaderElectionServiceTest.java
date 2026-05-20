package com.example.marketdata.leader;

import com.example.marketdata.AbstractIntegrationTest;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class LeaderElectionServiceTest extends AbstractIntegrationTest {

    @Autowired LeaderElectionService leader;

    @Test
    void singlePodEventuallyBecomesLeader() {
        Awaitility.await()
                .atMost(Duration.ofSeconds(10))
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> assertThat(leader.isLeader()).isTrue());

        assertThat(leader.getState()).isEqualTo(LeaderState.LEADER);
        assertThat(leader.getRole()).isEqualTo(PodRole.WARM_ELIGIBLE);
    }
}
