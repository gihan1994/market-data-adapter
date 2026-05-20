package com.example.marketdata.subscription;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Kafka payload on {@code market-data-requests}.
 * <pre>
 *   { "ric": "EUR=", "action": "SUBSCRIBE", "requester": "trading-ms" }
 * </pre>
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SubscriptionRequest implements Serializable {
    @NotBlank private String ric;
    @NotNull  private SubscriptionAction action;
    @NotBlank private String requester;
}
