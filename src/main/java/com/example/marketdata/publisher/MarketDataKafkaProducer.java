package com.example.marketdata.publisher;

import com.example.marketdata.config.MarketDataProperties;
import com.example.marketdata.lseg.MarketDataTick;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;

/**
 * Publishes ticks and gap events to Kafka. Uses transactional producer so that
 * a single business operation (e.g. snapshot + state change audit) is atomic.
 */
@Service
@Slf4j
public class MarketDataKafkaProducer {

    private final KafkaTemplate<String, Object> kafka;
    private final MarketDataProperties props;
    private final Counter ticksPublished;
    private final Counter gapsPublished;
    private final Counter publishFailures;
    private final Timer publishLatency;

    public MarketDataKafkaProducer(KafkaTemplate<String, Object> kafka,
                                   MarketDataProperties props,
                                   MeterRegistry meters) {
        this.kafka = kafka;
        this.props = props;
        this.ticksPublished  = Counter.builder("marketdata.kafka.ticks.published").register(meters);
        this.gapsPublished   = Counter.builder("marketdata.kafka.gaps.published").register(meters);
        this.publishFailures = Counter.builder("marketdata.kafka.publish.failures").register(meters);
        this.publishLatency  = Timer.builder("marketdata.kafka.publish.latency").register(meters);
    }

    /** Publish a live tick (or snapshot) to market-data-updates. */
    public void publishTick(MarketDataTick tick) {
        MarketDataEvent event = MarketDataEvent.builder()
                .ric(tick.getRic())
                .type(mapType(tick.getType()))
                .bid(tick.getBid())
                .ask(tick.getAsk())
                .last(tick.getLast())
                .volume(tick.getVolume())
                .sourceTimestampMs(tick.getSourceTimestamp() != null
                        ? tick.getSourceTimestamp().toEpochMilli() : 0)
                .publishedAtMs(Instant.now().toEpochMilli())
                .sourcePod(props.getPod().getName())
                .statusText(tick.getStatusText())
                .build();

        Timer.Sample sample = Timer.start();
        CompletableFuture<SendResult<String, Object>> future = kafka.send(
                props.getKafka().getTopicUpdates(), tick.getRic(), event);

        future.whenComplete((result, ex) -> {
            sample.stop(publishLatency);
            if (ex != null) {
                publishFailures.increment();
                log.error("Publish failed for ric={}: {}", tick.getRic(), ex.getMessage());
            } else {
                ticksPublished.increment();
                logSendSuccess(result);
            }
        });
    }

    /** Publish a previously-cached snapshot (used when a 2nd MS subscribes to a known RIC). */
    public void publishCachedSnapshot(String ric, String cachedJson) {
        // Forward raw cached payload as a STATUS event with snapshot semantics — downstream
        // can decide whether to merge against its own state.
        MarketDataEvent event = MarketDataEvent.builder()
                .ric(ric)
                .type(MarketDataEvent.EventType.SNAPSHOT)
                .publishedAtMs(Instant.now().toEpochMilli())
                .sourcePod(props.getPod().getName())
                .statusText(cachedJson)
                .build();
        kafka.send(props.getKafka().getTopicUpdates(), ric, event);
        ticksPublished.increment();
    }

    /** Publish a gap or recovery event to market-data-control. */
    @Transactional   // tx-aware so this can be enrolled with DB writes via ChainedKafkaTransactionManager (future)
    public void publishGap(GapEvent gap) {
        gap.setPublishedAtMs(Instant.now().toEpochMilli());
        kafka.send(props.getKafka().getTopicControl(), gap.getRic(), gap);
        gapsPublished.increment();
        log.info("Published {} ric={} duration={}ms", gap.getKind(), gap.getRic(), gap.getDurationMs());
    }

    private static MarketDataEvent.EventType mapType(MarketDataTick.Type t) {
        return switch (t) {
            case SNAPSHOT -> MarketDataEvent.EventType.SNAPSHOT;
            case UPDATE   -> MarketDataEvent.EventType.UPDATE;
            case STATUS   -> MarketDataEvent.EventType.STATUS;
        };
    }

    private static void logSendSuccess(SendResult<String, Object> r) {
        RecordMetadata md = r.getRecordMetadata();
        if (log.isDebugEnabled()) {
            log.debug("Sent topic={} partition={} offset={}", md.topic(), md.partition(), md.offset());
        }
    }
}
