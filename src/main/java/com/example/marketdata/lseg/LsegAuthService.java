package com.example.marketdata.lseg;

import com.example.marketdata.config.MarketDataProperties;
import com.example.marketdata.config.MarketDataProperties.Lseg.ConnectionMode;
import com.example.marketdata.domain.AuditEventType;
import com.example.marketdata.domain.SubscriptionAuditEntity;
import com.example.marketdata.repository.SubscriptionAuditRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;
import org.springframework.http.*;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.util.StringUtils;
import org.springframework.web.client.RestClient;

import java.time.Duration;
import java.time.Instant;

/**
 * OAuth2 V2 token management — used <strong>only</strong> for the RTO cloud connection mode.
 *
 * <p>For on-prem TREP (the production deployment), DACS authentication is performed by EMA
 * itself in the LOGIN domain handshake — there is no separate OAuth flow. This service
 * becomes a no-op when {@code marketdata.lseg.connection-mode = ON_PREM_TREP}.
 *
 * <p>When active (RTO mode), this service:
 * <ul>
 *   <li>fetches tokens via {@code client_credentials} grant</li>
 *   <li>caches them in Redis (diagnostic only — EMA SDK still manages its own token internally)</li>
 *   <li>proactively refreshes 5 minutes before expiry</li>
 * </ul>
 */
@Service
@Slf4j
public class LsegAuthService {

    private static final String REDIS_TOKEN_KEY = "marketdata:lseg:auth:token";

    private final MarketDataProperties props;
    private final RedissonClient redisson;
    private final SubscriptionAuditRepository auditRepo;
    private final ObjectMapper mapper = new ObjectMapper();
    private RestClient restClient;

    public LsegAuthService(MarketDataProperties props,
                           RedissonClient redisson,
                           SubscriptionAuditRepository auditRepo) {
        this.props = props;
        this.redisson = redisson;
        this.auditRepo = auditRepo;
    }

    @PostConstruct
    void init() {
        if (props.getLseg().getConnectionMode() == ConnectionMode.ON_PREM_TREP) {
            log.info("ON_PREM_TREP mode — OAuth2 token service is disabled (DACS auth used instead)");
            return;
        }
        this.restClient = RestClient.builder()
                .baseUrl(props.getLseg().getTokenUrl())
                .build();
    }

    private boolean isActive() {
        return props.getLseg().getConnectionMode() == ConnectionMode.RTO_CLOUD
                && !props.getLseg().isMockMode();
    }

    /**
     * Synchronously fetch a fresh OAuth2 token via client_credentials grant.
     */
    public AuthToken fetchToken() {
        if (!isActive()) {
            log.debug("OAuth2 disabled (ON_PREM_TREP or mock mode) — returning placeholder token");
            return AuthToken.builder()
                    .accessToken("n/a-on-prem-trep")
                    .tokenType("Bearer")
                    .expiresInSeconds(3600)
                    .acquiredAtEpochMs(Instant.now().toEpochMilli())
                    .build();
        }
        if (props.getLseg().isMockMode()) {
            log.debug("Mock mode — returning fake token");
            return AuthToken.builder()
                    .accessToken("mock-token-" + Instant.now().toEpochMilli())
                    .tokenType("Bearer")
                    .expiresInSeconds(3600)
                    .acquiredAtEpochMs(Instant.now().toEpochMilli())
                    .build();
        }

        if (!StringUtils.hasText(props.getLseg().getClientId())) {
            throw new IllegalStateException("LSEG client-id not configured (marketdata.lseg.client-id)");
        }

        MultiValueMap<String, String> body = new LinkedMultiValueMap<>();
        body.add("grant_type", "client_credentials");
        body.add("client_id", props.getLseg().getClientId());
        body.add("client_secret", props.getLseg().getClientSecret());
        body.add("scope", props.getLseg().getScope());

        try {
            AuthToken token = restClient.post()
                    .contentType(MediaType.APPLICATION_FORM_URLENCODED)
                    .accept(MediaType.APPLICATION_JSON)
                    .body(body)
                    .retrieve()
                    .body(AuthToken.class);

            if (token == null || token.getAccessToken() == null) {
                throw new IllegalStateException("LSEG token response was empty");
            }
            token.setAcquiredAtEpochMs(Instant.now().toEpochMilli());

            // Cache in Redis with TTL slightly shorter than the actual expiry.
            long ttl = Math.max(30, token.getExpiresInSeconds() - 60);
            RBucket<String> bucket = redisson.getBucket(REDIS_TOKEN_KEY);
            bucket.set(mapper.writeValueAsString(token), Duration.ofSeconds(ttl));

            log.info("Fetched LSEG token (expires in {}s, cached in Redis for {}s)",
                    token.getExpiresInSeconds(), ttl);
            audit(AuditEventType.TOKEN_REFRESHED, "expires_in=" + token.getExpiresInSeconds());
            return token;
        } catch (Exception e) {
            log.error("Failed to fetch LSEG token: {}", e.getMessage());
            audit(AuditEventType.TOKEN_REFRESHED, "FAILED: " + e.getMessage());
            throw new IllegalStateException("LSEG token fetch failed", e);
        }
    }

    /**
     * Get cached token from Redis if still valid, otherwise fetch a new one.
     */
    public AuthToken getOrFetch() {
        try {
            RBucket<String> bucket = redisson.getBucket(REDIS_TOKEN_KEY);
            String cached = bucket.get();
            if (cached != null) {
                AuthToken token = mapper.readValue(cached, AuthToken.class);
                if (!token.isExpiringSoon(300)) return token;
            }
        } catch (Exception e) {
            log.debug("Cache read failed: {}", e.getMessage());
        }
        return fetchToken();
    }

    /** Refresh proactively before expiry. Active leader runs this. */
    @Scheduled(fixedDelay = 60_000)
    void scheduledRefresh() {
        if (!isActive()) return;
        try {
            RBucket<String> bucket = redisson.getBucket(REDIS_TOKEN_KEY);
            String cached = bucket.get();
            if (cached == null) {
                log.debug("No cached token — skipping proactive refresh");
                return;
            }
            AuthToken token = mapper.readValue(cached, AuthToken.class);
            if (token.isExpiringSoon(300)) {
                log.info("Token expiring soon — proactively refreshing");
                fetchToken();
            }
        } catch (Exception e) {
            log.warn("Scheduled token refresh check failed: {}", e.getMessage());
        }
    }

    private void audit(AuditEventType type, String detail) {
        try {
            auditRepo.save(SubscriptionAuditEntity.builder()
                    .podName(props.getPod().getName())
                    .hall(props.getPod().getHall())
                    .eventType(type)
                    .detail(detail)
                    .build());
        } catch (Exception ignored) { /* don't fail auth on audit failure */ }
    }
}
