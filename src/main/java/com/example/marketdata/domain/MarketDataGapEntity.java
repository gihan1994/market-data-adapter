package com.example.marketdata.domain;

import jakarta.persistence.*;
import lombok.*;

import java.time.OffsetDateTime;

@Entity
@Table(name = "market_data_gaps")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@ToString
public class MarketDataGapEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "ric", nullable = false, length = 64)
    private String ric;

    @Column(name = "gap_start", nullable = false)
    private OffsetDateTime gapStart;

    @Column(name = "gap_end", nullable = false)
    private OffsetDateTime gapEnd;

    @Column(name = "duration_ms", nullable = false)
    private long durationMs;

    @Column(name = "detected_by", nullable = false, length = 64)
    private String detectedBy;

    @Column(name = "published", nullable = false)
    private boolean published;

    @Column(name = "created_at", nullable = false)
    private OffsetDateTime createdAt;

    @PrePersist
    void prePersist() {
        if (createdAt == null) createdAt = OffsetDateTime.now();
    }
}
