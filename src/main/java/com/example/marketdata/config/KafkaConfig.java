package com.example.marketdata.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.*;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.context.annotation.Primary;

import jakarta.persistence.EntityManagerFactory;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
public class KafkaConfig {

    private final KafkaProperties kafkaProperties;
    private final MarketDataProperties props;

    @Value("${marketdata.pod.name}")
    private String podName;

    public KafkaConfig(KafkaProperties kafkaProperties, MarketDataProperties props) {
        this.kafkaProperties = kafkaProperties;
        this.props = props;
    }

    // ---------- Topics ----------
    @Bean
    NewTopic topicRequests() {
        return TopicBuilder.name(props.getKafka().getTopicRequests())
                .partitions(6).replicas(3)
                .config("retention.ms", "604800000")    // 7d
                .build();
    }

    @Bean
    NewTopic topicUpdates() {
        return TopicBuilder.name(props.getKafka().getTopicUpdates())
                .partitions(12).replicas(3)
                .config("compression.type", "lz4")
                .config("retention.ms", "86400000")     // 24h — ticks are ephemeral
                .build();
    }

    @Bean
    NewTopic topicControl() {
        return TopicBuilder.name(props.getKafka().getTopicControl())
                .partitions(3).replicas(3)
                .config("retention.ms", "2592000000")   // 30d — control history kept longer
                .build();
    }

    // ---------- Producer (transactional) ----------
    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> cfg = new HashMap<>(kafkaProperties.buildProducerProperties(null));
        cfg.put("key.serializer", StringSerializer.class);
        cfg.put("value.serializer", JsonSerializer.class);
        // Unique transactional id per pod so multiple instances don't fence each other.
        cfg.put("transactional.id", "market-data-tx-" + podName);
        cfg.put("enable.idempotence", true);
        DefaultKafkaProducerFactory<String, Object> pf = new DefaultKafkaProducerFactory<>(cfg);
        pf.setTransactionIdPrefix("md-tx-" + podName + "-");
        return pf;
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<String, Object> pf) {
        return new KafkaTemplate<>(pf);
    }

    @Bean
    public KafkaTransactionManager<String, Object> kafkaTransactionManager(ProducerFactory<String, Object> pf) {
        return new KafkaTransactionManager<>(pf);
    }

    /**
     * Explicit JPA transaction manager. Spring Boot's auto-config skips creating one
     * because the KafkaTransactionManager above already satisfies PlatformTransactionManager.
     * We mark JPA as @Primary so @Transactional on JPA repositories picks it by default.
     */
    @Bean(name = "transactionManager")
    @Primary
    public PlatformTransactionManager jpaTransactionManager(EntityManagerFactory emf) {
        return new JpaTransactionManager(emf);
    }

    // ---------- Consumer (for market-data-requests) ----------
    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        Map<String, Object> cfg = new HashMap<>(kafkaProperties.buildConsumerProperties(null));
        cfg.put("key.deserializer", StringDeserializer.class);
        cfg.put("value.deserializer", ErrorHandlingDeserializer.class);
        cfg.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class.getName());
        cfg.put(JsonDeserializer.TRUSTED_PACKAGES, "com.example.marketdata.*");
        cfg.put(JsonDeserializer.VALUE_DEFAULT_TYPE,
                "com.example.marketdata.subscription.SubscriptionRequest");
        cfg.put("isolation.level", "read_committed");
        return new DefaultKafkaConsumerFactory<>(cfg);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
            ConsumerFactory<String, Object> cf) {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(cf);
        factory.setBatchListener(false);
        factory.getContainerProperties().setAckMode(
                org.springframework.kafka.listener.ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        return factory;
    }
}
