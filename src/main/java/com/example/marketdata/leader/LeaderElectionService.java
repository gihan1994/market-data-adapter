package com.example.marketdata.leader;

import com.example.marketdata.config.MarketDataProperties;
import com.example.marketdata.domain.AuditEventType;
import com.example.marketdata.domain.SubscriptionAuditEntity;
import com.example.marketdata.repository.SubscriptionAuditRepository;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Redis-backed leader election using a single key with TTL (instead of Redisson's RLock,
 * because we need to identify the current holder and renew explicitly).
 *
 * Algorithm per scheduled tick:
 *   - if leader: extend TTL on the lock; on failure → DemotedEvent, transition to COMPETING
 *   - if not leader: try SET NX value=podName EX ttl; on success → PromotedToLeaderEvent
 *
 * Tick interval = heartbeatSeconds.
 */
@Service
@Slf4j
public class LeaderElectionService {

    private final RedissonClient redisson;
    private final MarketDataProperties props;
    private final ApplicationEventPublisher events;
    private final SubscriptionAuditRepository auditRepo;

    private final AtomicReference<LeaderState> state = new AtomicReference<>(LeaderState.STARTING);
    private final PodRole role;
    private final String podName;
    private final String hall;
    /** Identity stored in the lock value — includes hall for cross-cluster diagnostics. */
    private final String lockHolderId;
    private final String lockKey;
    private final int ttlSeconds;

    public LeaderElectionService(RedissonClient redisson,
                                 MarketDataProperties props,
                                 ApplicationEventPublisher events,
                                 SubscriptionAuditRepository auditRepo) {
        this.redisson = redisson;
        this.props = props;
        this.events = events;
        this.auditRepo = auditRepo;
        this.role = PodRole.fromString(props.getPod().getRole());
        this.podName = props.getPod().getName();
        this.hall = props.getPod().getHall();
        this.lockHolderId = hall + "/" + podName;   // e.g. "hall1/market-data-service-0"
        this.lockKey = props.getLeader().getLockKey();
        this.ttlSeconds = props.getLeader().getLockTtlSeconds();
    }

    @PostConstruct
    void init() {
        log.info("LeaderElectionService init: hall={} pod={} role={} lockKey={} ttl={}s",
                hall, podName, role, lockKey, ttlSeconds);
        state.set(role == PodRole.COLD_ONLY ? LeaderState.COLD_STANDBY : LeaderState.COMPETING);
        audit(AuditEventType.POD_STARTED, "hall=" + hall + " role=" + role);
    }

    /**
     * Periodic election + heartbeat. Runs every heartbeatSeconds.
     */
    @Scheduled(fixedDelayString = "#{${marketdata.leader.heartbeat-seconds} * 1000}")
    public void tick() {
        if (state.get() == LeaderState.TERMINATING) return;

        try {
            RBucket<String> bucket = redisson.getBucket(lockKey);

            if (isLeader()) {
                // Renew TTL. If key gone (e.g. Redis flushed), we lost leadership.
                String current = bucket.get();
                if (lockHolderId.equals(current)) {
                    bucket.expire(Duration.ofSeconds(ttlSeconds));
                } else {
                    log.warn("Lost leadership: lock now held by '{}'", current);
                    transitionTo(LeaderState.COMPETING, "lost lock during heartbeat");
                }
            } else {
                // Try to acquire.
                boolean acquired = bucket.setIfAbsent(lockHolderId, Duration.ofSeconds(ttlSeconds));
                if (acquired) {
                    transitionTo(LeaderState.LEADER, "lock acquired");
                } else if (state.get() == LeaderState.COMPETING) {
                    // Stay in COMPETING — but for visibility, demote warm-eligible pods to WARM_STANDBY here.
                    transitionTo(role == PodRole.WARM_ELIGIBLE
                            ? LeaderState.WARM_STANDBY
                            : LeaderState.COLD_STANDBY,
                            "leader held by another pod: " + bucket.get());
                }
            }
        } catch (Exception e) {
            log.error("Leader election tick failed", e);
            if (isLeader()) {
                transitionTo(LeaderState.DEGRADED, "redis error: " + e.getMessage());
            }
        }
    }

    private void transitionTo(LeaderState newState, String reason) {
        LeaderState old = state.getAndSet(newState);
        if (old == newState) return;
        log.info("State transition: {} → {} ({})", old, newState, reason);

        if (newState == LeaderState.LEADER) {
            audit(AuditEventType.LEADER_ACQUIRED, reason);
            events.publishEvent(new PromotedToLeaderEvent(this, podName));
        } else if (old == LeaderState.LEADER) {
            audit(AuditEventType.LEADER_LOST, reason);
            events.publishEvent(new DemotedEvent(this, podName, reason));
        }
    }

    @PreDestroy
    void shutdown() {
        log.info("Shutting down — releasing leader lock if held");
        state.set(LeaderState.TERMINATING);
        try {
            RBucket<String> bucket = redisson.getBucket(lockKey);
            String current = bucket.get();
            if (lockHolderId.equals(current)) {
                bucket.delete();
                log.info("Released leader lock");
            }
        } catch (Exception e) {
            log.warn("Failed to release leader lock on shutdown: {}", e.getMessage());
        }
        audit(AuditEventType.POD_TERMINATING, null);
    }

    private void audit(AuditEventType type, String detail) {
        try {
            auditRepo.save(SubscriptionAuditEntity.builder()
                    .podName(podName)
                    .hall(hall)
                    .eventType(type)
                    .detail(detail)
                    .build());
        } catch (Exception e) {
            log.debug("Audit write failed: {}", e.getMessage());
        }
    }

    // ------------ public API ------------
    public boolean isLeader() { return state.get() == LeaderState.LEADER; }
    public LeaderState getState() { return state.get(); }
    public PodRole getRole() { return role; }
    public String getPodName() { return podName; }
    public String getHall() { return hall; }
    public String getLockHolderId() { return lockHolderId; }
}
