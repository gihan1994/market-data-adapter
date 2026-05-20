package com.example.marketdata.subscription;

/**
 * Lifecycle state of a RIC subscription. Persisted in ric_registry.state.
 * Transitions are documented in page 6 of the architecture diagram.
 */
public enum SubscriptionState {
    REQUESTED,
    SUBSCRIBING,
    ACTIVE,
    DRAINING,
    CLOSED,
    ERROR
}
