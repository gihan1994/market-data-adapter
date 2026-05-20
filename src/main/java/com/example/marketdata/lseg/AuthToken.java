package com.example.marketdata.lseg;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.Instant;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class AuthToken implements Serializable {

    @JsonProperty("access_token")
    private String accessToken;

    @JsonProperty("refresh_token")
    private String refreshToken;

    @JsonProperty("expires_in")
    private long expiresInSeconds;

    @JsonProperty("token_type")
    private String tokenType;

    /** When this token was acquired (UTC ms). */
    private long acquiredAtEpochMs;

    public boolean isExpiringSoon(int marginSeconds) {
        long expiresAt = acquiredAtEpochMs + (expiresInSeconds * 1000);
        return Instant.now().toEpochMilli() > (expiresAt - marginSeconds * 1000L);
    }
}
