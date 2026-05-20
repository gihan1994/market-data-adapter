package com.example.marketdata.domain;

import com.example.marketdata.subscription.SubscriptionState;
import jakarta.persistence.*;
import lombok.*;

import java.time.OffsetDateTime;

@Entity
@Table(name = "ric_registry")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@ToString
public class RicRegistryEntity {

    @Id
    @Column(name = "ric", length = 64)
    private String ric;

    @Column(name = "service_name", nullable = false, length = 64)
    private String serviceName;

    @Column(name = "active", nullable = false)
    private boolean active;

    @Column(name = "first_subscribed", nullable = false)
    private OffsetDateTime firstSubscribed;

    @Column(name = "last_state_change", nullable = false)
    private OffsetDateTime lastStateChange;

    @Enumerated(EnumType.STRING)
    @Column(name = "state", nullable = false, length = 32)
    private SubscriptionState state;

    @PrePersist
    void prePersist() {
        OffsetDateTime now = OffsetDateTime.now();
        if (firstSubscribed == null) firstSubscribed = now;
        if (lastStateChange == null) lastStateChange = now;
        if (state == null) state = SubscriptionState.REQUESTED;
    }
}
