package com.example.marketdata.repository;

import com.example.marketdata.domain.RicRegistryEntity;
import com.example.marketdata.subscription.SubscriptionState;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;

@Repository
public interface RicRegistryRepository extends JpaRepository<RicRegistryEntity, String> {

    List<RicRegistryEntity> findAllByActiveTrue();

    Optional<RicRegistryEntity> findByRicAndActiveTrue(String ric);

    @Modifying
    @Query("update RicRegistryEntity r set r.state = :state, r.lastStateChange = :ts where r.ric = :ric")
    int updateState(@Param("ric") String ric,
                    @Param("state") SubscriptionState state,
                    @Param("ts") OffsetDateTime ts);

    @Modifying
    @Query("update RicRegistryEntity r set r.active = false, r.state = com.example.marketdata.subscription.SubscriptionState.CLOSED, r.lastStateChange = :ts where r.ric = :ric")
    int deactivate(@Param("ric") String ric, @Param("ts") OffsetDateTime ts);
}
