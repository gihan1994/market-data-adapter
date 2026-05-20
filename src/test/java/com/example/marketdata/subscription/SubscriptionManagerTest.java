package com.example.marketdata.subscription;

import com.example.marketdata.AbstractIntegrationTest;
import com.example.marketdata.leader.LeaderElectionService;
import com.example.marketdata.lseg.OmmConsumerManager;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class SubscriptionManagerTest extends AbstractIntegrationTest {

    @Autowired SubscriptionManager manager;
    @Autowired LeaderElectionService leader;
    @Autowired OmmConsumerManager omm;
    @Autowired RefcountService refcount;
    @Autowired RicRegistryService registry;

    @Test
    void firstSubscribeTriggersEmaCall_secondDoesNot() {
        // Wait until we are the leader
        Awaitility.await().atMost(Duration.ofSeconds(10)).until(leader::isLeader);

        String ric = "EUR=TEST-" + System.nanoTime();

        // 1st subscriber
        manager.handleSubscribe(new SubscriptionRequest(ric, SubscriptionAction.SUBSCRIBE, "trading-ms"));
        Awaitility.await().atMost(Duration.ofSeconds(2)).untilAsserted(() ->
                assertThat(omm.currentSubscriptions()).containsKey(ric));
        assertThat(refcount.get(ric)).isEqualTo(1);

        // 2nd subscriber (different requester) — still only 1 EMA subscription
        manager.handleSubscribe(new SubscriptionRequest(ric, SubscriptionAction.SUBSCRIBE, "risk-ms"));
        assertThat(refcount.get(ric)).isEqualTo(2);
        assertThat(omm.currentSubscriptions()).containsKey(ric);
    }

    @Test
    void unsubscribeFromAllDrainsEventually() {
        Awaitility.await().atMost(Duration.ofSeconds(10)).until(leader::isLeader);

        String ric = "USD=TEST-" + System.nanoTime();
        manager.handleSubscribe(new SubscriptionRequest(ric, SubscriptionAction.SUBSCRIBE, "trading-ms"));
        Awaitility.await().atMost(Duration.ofSeconds(2)).until(() -> omm.currentSubscriptions().containsKey(ric));

        manager.handleUnsubscribe(new SubscriptionRequest(ric, SubscriptionAction.UNSUBSCRIBE, "trading-ms"));

        // drainGrace=2s in application-test.yml — after that, the stream should be closed
        Awaitility.await().atMost(Duration.ofSeconds(8)).untilAsserted(() ->
                assertThat(omm.currentSubscriptions()).doesNotContainKey(ric));
        assertThat(refcount.get(ric)).isZero();
    }
}
