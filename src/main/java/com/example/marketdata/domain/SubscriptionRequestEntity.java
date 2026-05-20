package com.example.marketdata.domain;

import jakarta.persistence.*;
import lombok.*;

import java.time.OffsetDateTime;

@Entity
@Table(name = "subscription_requests",
        uniqueConstraints = @UniqueConstraint(columnNames = {"business_ms", "ric"}))
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@ToString
public class SubscriptionRequestEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "business_ms", nullable = false, length = 64)
    private String businessMs;

    @Column(name = "ric", nullable = false, length = 64)
    private String ric;

    @Column(name = "requested_at", nullable = false)
    private OffsetDateTime requestedAt;

    @Column(name = "active", nullable = false)
    private boolean active;

    @PrePersist
    void prePersist() {
        if (requestedAt == null) requestedAt = OffsetDateTime.now();
    }
}
