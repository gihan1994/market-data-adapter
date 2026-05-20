package com.example.marketdata.publisher;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Kafka payload on {@code market-data-control}. Signals to downstream consumers
 * that ticks were missed between gapStartMs and gapEndMs for a specific RIC.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class GapEvent implements Serializable {

    public enum Kind { GAP_DETECTED, RECOVERY_COMPLETE, LEADER_CHANGE }

    private Kind kind;
    private String ric;
    private long gapStartMs;
    private long gapEndMs;
    private long durationMs;
    private String newLeaderPod;
    private String reason;
    private long publishedAtMs;
}
