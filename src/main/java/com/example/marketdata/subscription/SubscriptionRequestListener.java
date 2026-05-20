package com.example.marketdata.subscription;

import com.example.marketdata.leader.DemotedEvent;
import com.example.marketdata.leader.LeaderElectionService;
import com.example.marketdata.leader.PromotedToLeaderEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Kafka consumer for {@code market-data-requests}.
 *
 * <h2>Leader-only consumption</h2>
 * The listener container is created with {@code autoStartup = "false"} — Spring registers
 * it but does NOT start consuming. The container is started / stopped explicitly when this
 * pod is promoted to or demoted from leader.
 *
 * <p>This means only the leader pod is a member of the Kafka consumer group, so it
 * receives ALL partitions of the {@code market-data-requests} topic. Non-leader pods
 * hold no Kafka subscription and burn no broker resources.
 *
 * <p>During failover (~5s lock TTL + ~5s Kafka rebalance), requests pile up in the topic;
 * the new leader picks them up from its committed offset when it starts the listener.
 */
@Component
@Slf4j
public class SubscriptionRequestListener {

    /** Must match the {@code id} attribute on the {@code @KafkaListener} below. */
    public static final String LISTENER_ID = "subscription-request-listener";

    private final SubscriptionManager manager;
    private final LeaderElectionService leader;
    private final KafkaListenerEndpointRegistry registry;

    public SubscriptionRequestListener(SubscriptionManager manager,
                                       LeaderElectionService leader,
                                       KafkaListenerEndpointRegistry registry) {
        this.manager = manager;
        this.leader = leader;
        this.registry = registry;
    }

    @KafkaListener(
            id = LISTENER_ID,
            topics = "${marketdata.kafka.topic-requests}",
            groupId = "${spring.kafka.consumer.group-id}",
            autoStartup = "false",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onRequest(SubscriptionRequest req, Acknowledgment ack) {
        try {
            if (req == null || req.getRic() == null || req.getAction() == null) {
                log.warn("Dropping malformed request: {}", req);
                ack.acknowledge();
                return;
            }

            // Defensive: container should only be running when we're leader,
            // but guard against the brief window during rebalance.
            if (!leader.isLeader()) {
                log.warn("Received request while not leader (rebalance in progress?) — not acking, will be redelivered");
                return;
            }

            switch (req.getAction()) {
                case SUBSCRIBE   -> manager.handleSubscribe(req);
                case UNSUBSCRIBE -> manager.handleUnsubscribe(req);
            }
            ack.acknowledge();
        } catch (Exception e) {
            log.error("Failed to process request {}: {}", req, e.getMessage(), e);
            ack.acknowledge();   // ack to avoid poison-pill; failure is captured in audit
        }
    }

    // ============================================================
    //  Start/stop the listener based on leadership
    // ============================================================

    @EventListener
    public void onPromoted(PromotedToLeaderEvent event) {
        MessageListenerContainer container = registry.getListenerContainer(LISTENER_ID);
        if (container == null) {
            log.error("Kafka listener container '{}' not found — cannot start", LISTENER_ID);
            return;
        }
        if (!container.isRunning()) {
            log.info("Promoted to leader — starting Kafka listener for {}", container.getContainerProperties());
            container.start();
        }
    }

    @EventListener
    public void onDemoted(DemotedEvent event) {
        MessageListenerContainer container = registry.getListenerContainer(LISTENER_ID);
        if (container == null) return;
        if (container.isRunning()) {
            log.info("Demoted ({}) — stopping Kafka listener", event.getReason());
            container.stop();
        }
    }
}
