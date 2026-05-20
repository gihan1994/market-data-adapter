package com.example.marketdata.lseg;

/**
 * Listener registered by SubscriptionManager to receive ticks from EMA callbacks.
 * Implementations should be non-blocking — EMA dispatcher thread is hot.
 */
@FunctionalInterface
public interface TickListener {
    void onTick(MarketDataTick tick);
}
