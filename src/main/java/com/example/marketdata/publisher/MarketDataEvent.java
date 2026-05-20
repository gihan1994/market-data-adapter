package com.example.marketdata.publisher;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.math.BigDecimal;

/**
 * Kafka payload on {@code market-data-updates}.
 * Keyed by RIC for ordered delivery per symbol.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class MarketDataEvent implements Serializable {

    public enum EventType { SNAPSHOT, UPDATE, STATUS }

    private String ric;
    private EventType type;
    private BigDecimal bid;
    private BigDecimal ask;
    private BigDecimal last;
    private Long volume;
    private long sourceTimestampMs;
    private long publishedAtMs;
    private String sourcePod;
    private String statusText;
}
