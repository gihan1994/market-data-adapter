package com.example.marketdata.publisher;

import com.example.marketdata.AbstractIntegrationTest;
import com.example.marketdata.config.MarketDataProperties;
import com.example.marketdata.lseg.MarketDataTick;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class MarketDataKafkaProducerTest extends AbstractIntegrationTest {

    @Autowired MarketDataKafkaProducer producer;
    @Autowired KafkaTemplate<String, Object> kafkaTemplate;
    @Autowired MarketDataProperties props;

    @Test
    void publishesTickToUpdatesTopic() {
        String ric = "PUB-RIC-" + System.nanoTime();
        MarketDataTick tick = MarketDataTick.builder()
                .ric(ric)
                .type(MarketDataTick.Type.UPDATE)
                .bid(new BigDecimal("1.0852"))
                .ask(new BigDecimal("1.0853"))
                .receivedAt(Instant.now())
                .build();

        try (Consumer<String, MarketDataEvent> consumer = newConsumer(props.getKafka().getTopicUpdates())) {
            kafkaTemplate.executeInTransaction(t -> {
                producer.publishTick(tick);
                return true;
            });

            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                var records = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(1));
                boolean found = false;
                for (ConsumerRecord<String, MarketDataEvent> r : records) {
                    if (ric.equals(r.key())) {
                        assertThat(r.value().getBid()).isEqualByComparingTo("1.0852");
                        found = true;
                    }
                }
                assertThat(found).isTrue();
            });
        }
    }

    private Consumer<String, MarketDataEvent> newConsumer(String topic) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-" + System.nanoTime());
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
        p.put(JsonDeserializer.TRUSTED_PACKAGES, "com.example.marketdata.*");
        p.put(JsonDeserializer.VALUE_DEFAULT_TYPE, MarketDataEvent.class.getName());
        p.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        KafkaConsumer<String, MarketDataEvent> c = new KafkaConsumer<>(p);
        c.subscribe(java.util.List.of(topic));
        return c;
    }
}
