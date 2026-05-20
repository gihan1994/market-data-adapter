package com.example.marketdata.repository;

import com.example.marketdata.domain.SubscriptionAuditEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface SubscriptionAuditRepository extends JpaRepository<SubscriptionAuditEntity, Long> {
}
