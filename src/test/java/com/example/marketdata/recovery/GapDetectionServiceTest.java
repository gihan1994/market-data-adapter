package com.example.marketdata.recovery;

import com.example.marketdata.AbstractIntegrationTest;
import com.example.marketdata.config.MarketDataProperties;
import com.example.marketdata.publisher.GapEvent;
import org.junit.jupiter.api.Test;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Duration;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class GapDetectionServiceTest extends AbstractIntegrationTest {

    @Autowired GapDetectionService gapDetection;
    @Autowired RedissonClient redisson;
    @Autowired MarketDataProperties props;

    @Test
    void detectsGapWhenLastPublishedExceedsThreshold() {
        String ric = "GAP-RIC-" + System.nanoTime();
        long now = System.currentTimeMillis();
        long fiveSecondsAgo = now - 5_000;

        redisson.getBucket(props.getSubscription().getLastPublishedKeyPrefix() + ric)
                .set(String.valueOf(fiveSecondsAgo), Duration.ofMinutes(1));

        Optional<GapEvent> maybeGap = gapDetection.computeGap(ric, now, 2_000);
        assertThat(maybeGap).isPresent();
        assertThat(maybeGap.get().getDurationMs()).isGreaterThanOrEqualTo(5_000);
        assertThat(maybeGap.get().getKind()).isEqualTo(GapEvent.Kind.GAP_DETECTED);
    }

    @Test
    void noGapWhenWithinThreshold() {
        String ric = "NOGAP-RIC-" + System.nanoTime();
        long now = System.currentTimeMillis();
        redisson.getBucket(props.getSubscription().getLastPublishedKeyPrefix() + ric)
                .set(String.valueOf(now - 500), Duration.ofMinutes(1));

        Optional<GapEvent> maybeGap = gapDetection.computeGap(ric, now, 2_000);
        assertThat(maybeGap).isEmpty();
    }

    @Test
    void noGapWhenNoPriorTimestamp() {
        Optional<GapEvent> maybeGap = gapDetection.computeGap(
                "NEVER-PUBLISHED-" + System.nanoTime(),
                System.currentTimeMillis(),
                2_000);
        assertThat(maybeGap).isEmpty();
    }
}
