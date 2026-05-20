package com.example.marketdata.leader;

import org.springframework.context.ApplicationEvent;

/**
 * Published when this pod acquires the leader lock. Listened to by
 * SubscriptionManager (starts EMA subscriptions) and RecoveryService (computes gaps).
 */
public class PromotedToLeaderEvent extends ApplicationEvent {
    private final String podName;
    public PromotedToLeaderEvent(Object source, String podName) {
        super(source);
        this.podName = podName;
    }
    public String getPodName() { return podName; }
}
