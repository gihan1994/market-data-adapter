package com.example.marketdata.lseg;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Internal tick representation extracted from an EMA RefreshMsg or UpdateMsg.
 * Independent from the Kafka {@code MarketDataEvent} so consumers can be
 * versioned separately from internal callbacks.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class MarketDataTick {

    public enum Type { SNAPSHOT, UPDATE, STATUS }

    private String ric;
    private Type type;
    private BigDecimal bid;
    private BigDecimal ask;
    private BigDecimal last;
    private Long volume;
    private Instant sourceTimestamp;     // exchange / LSEG ts if available
    private Instant receivedAt;          // adapter wall-clock
    private String statusText;           // populated for Type.STATUS
    private boolean complete;            // snapshot complete flag

    @Builder.Default
    private Map<String, Object> extra = new HashMap<>();
}
