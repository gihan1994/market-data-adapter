package com.example.marketdata.recovery;

import com.example.marketdata.config.MarketDataProperties;
import com.example.marketdata.leader.PromotedToLeaderEvent;
import com.example.marketdata.publisher.GapEvent;
import com.example.marketdata.publisher.MarketDataKafkaProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.time.Instant;

/**
 * Orchestrates recovery after this pod is promoted to leader:
 *   1. Emit LEADER_CHANGE control event
 *   2. Detect per-RIC gaps and emit GAP_DETECTED events
 *   3. Emit RECOVERY_COMPLETE once done
 *
 * Note: SubscriptionManager handles the actual resubscription in parallel
 * (separate @Async listener on PromotedToLeaderEvent).
 */
@Service
@Slf4j
public class RecoveryService {

    private final GapDetectionService gapDetection;
    private final MarketDataKafkaProducer producer;
    private final MarketDataProperties props;

    public RecoveryService(GapDetectionService gapDetection,
                           MarketDataKafkaProducer producer,
                           MarketDataProperties props) {
        this.gapDetection = gapDetection;
        this.producer = producer;
        this.props = props;
    }

    @EventListener
    @Async("resubscribeExecutor")
    public void onPromoted(PromotedToLeaderEvent event) {
        log.info("RecoveryService starting recovery flow for pod={}", event.getPodName());
        long start = Instant.now().toEpochMilli();

        // 1. Announce leader change on control topic
        producer.publishGap(GapEvent.builder()
                .kind(GapEvent.Kind.LEADER_CHANGE)
                .ric("*")
                .newLeaderPod(event.getPodName())
                .reason("promoted-to-leader")
                .build());

        // 2. Detect gaps for all active RICs
        int gaps;
        try {
            gaps = gapDetection.detectAndPublishAll();
        } catch (Exception e) {
            log.error("Gap detection failed: {}", e.getMessage(), e);
            gaps = -1;
        }

        // 3. Recovery complete
        long durationMs = Instant.now().toEpochMilli() - start;
        producer.publishGap(GapEvent.builder()
                .kind(GapEvent.Kind.RECOVERY_COMPLETE)
                .ric("*")
                .durationMs(durationMs)
                .newLeaderPod(event.getPodName())
                .reason("gaps_detected=" + gaps)
                .build());
        log.info("Recovery flow complete in {}ms ({} gaps detected)", durationMs, gaps);
    }
}
