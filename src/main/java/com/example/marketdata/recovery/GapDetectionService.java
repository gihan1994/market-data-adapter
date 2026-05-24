package com.example.marketdata.recovery;

import com.example.marketdata.config.MarketDataProperties;
import com.example.marketdata.domain.MarketDataGapEntity;
import com.example.marketdata.publisher.GapEvent;
import com.example.marketdata.publisher.MarketDataKafkaProducer;
import com.example.marketdata.repository.MarketDataGapRepository;
import com.example.marketdata.subscription.RicRegistryService;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;

/**
 * Detects per-RIC gaps based on Redis "last-published" timestamps.
 *
 * When this pod is promoted to leader, it iterates all active RICs and for each:
 *   - reads {@code ric:last-published:{ric}} from Redis
 *   - if the gap exceeds {@code recovery.gap-threshold-millis}, records a GapEvent
 *   - emits GAP_DETECTED on market-data-control
 */
@Service
@Slf4j
public class GapDetectionService {

    private final RicRegistryService registry;
    private final RedissonClient redisson;
    private final MarketDataProperties props;
    private final MarketDataKafkaProducer producer;
    private final MarketDataGapRepository gapRepo;

    public GapDetectionService(RicRegistryService registry,
                               RedissonClient redisson,
                               MarketDataProperties props,
                               MarketDataKafkaProducer producer,
                               MarketDataGapRepository gapRepo) {
        this.registry = registry;
        this.redisson = redisson;
        this.props = props;
        this.producer = producer;
        this.gapRepo = gapRepo;
    }

    /**
     * Compute gaps and publish for every active RIC. Returns the count of gaps detected.
     */
    @Transactional
    public int detectAndPublishAll() {
        long thresholdMs = props.getRecovery().getGapThresholdMillis();
        long now = Instant.now().toEpochMilli();
        int detected = 0;

        for (var entity : registry.findActiveRics()) {
            Optional<GapEvent> maybeGap = computeGap(entity.getRic(), now, thresholdMs);
            if (maybeGap.isPresent()) {
                GapEvent gap = maybeGap.get();
                gap.setNewLeaderPod(props.getPod().getName());
                gap.setReason("leader-transition");

                persistGap(gap);
                producer.publishGap(gap);
                detected++;
            }
        }
        log.info("Gap detection complete: {} gaps detected across {} active RICs",
                detected, registry.findActiveRics().size());
        return detected;
    }

    public Optional<GapEvent> computeGap(String ric, long nowMs, long thresholdMs) {
        try {
            var bucket = redisson.<String>getBucket(props.getSubscription().getLastPublishedKeyPrefix() + ric);
            String lastStr = bucket.get();
            if (lastStr == null) {
                // No previous tick — not a gap, just a fresh subscription
                return Optional.empty();
            }
            long lastPublished = Long.parseLong(lastStr);
            long duration = nowMs - lastPublished;
            if (duration < thresholdMs) return Optional.empty();
            return Optional.of(GapEvent.builder()
                    .kind(GapEvent.Kind.GAP_DETECTED)
                    .ric(ric)
                    .gapStartMs(lastPublished)
                    .gapEndMs(nowMs)
                    .durationMs(duration)
                    .build());
        } catch (Exception e) {
            log.debug("Gap compute failed for {}: {}", ric, e.getMessage());
            return Optional.empty();
        }
    }

    private void persistGap(GapEvent gap) {
        gapRepo.save(MarketDataGapEntity.builder()
                .ric(gap.getRic())
                .gapStart(OffsetDateTime.ofInstant(Instant.ofEpochMilli(gap.getGapStartMs()), ZoneOffset.UTC))
                .gapEnd(OffsetDateTime.ofInstant(Instant.ofEpochMilli(gap.getGapEndMs()), ZoneOffset.UTC))
                .durationMs(gap.getDurationMs())
                .detectedBy(gap.getNewLeaderPod())
                .hall(props.getPod().getHall())
                .published(true)
                .build());
    }
}
