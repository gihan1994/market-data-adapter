package com.example.marketdata.repository;

import com.example.marketdata.domain.MarketDataGapEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface MarketDataGapRepository extends JpaRepository<MarketDataGapEntity, Long> {
    List<MarketDataGapEntity> findByPublishedFalse();
}
