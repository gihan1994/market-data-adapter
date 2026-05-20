package com.example.marketdata.repository;

import com.example.marketdata.domain.SubscriptionRequestEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface SubscriptionRequestRepository extends JpaRepository<SubscriptionRequestEntity, Long> {

    Optional<SubscriptionRequestEntity> findByBusinessMsAndRic(String businessMs, String ric);

    List<SubscriptionRequestEntity> findByRicAndActiveTrue(String ric);

    long countByRicAndActiveTrue(String ric);

    @Modifying
    @Query("update SubscriptionRequestEntity s set s.active = false where s.businessMs = :ms and s.ric = :ric")
    int deactivate(@Param("ms") String businessMs, @Param("ric") String ric);
}
