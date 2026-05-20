package com.example.marketdata.leader;

/**
 * Static role assigned via POD_ROLE env var. Determines whether a non-leader pod
 * opens an LSEG session (warm) or stays disconnected (cold).
 */
public enum PodRole {
    /** StatefulSet ordinal 0, 1 — keeps an open EMA session as a 2nd login for fast failover. */
    WARM_ELIGIBLE,
    /** StatefulSet ordinal 2, 3 — no LSEG session unless promoted to leader. */
    COLD_ONLY;

    public static PodRole fromString(String s) {
        if (s == null) return WARM_ELIGIBLE;
        return switch (s.trim().toLowerCase()) {
            case "warm-eligible", "warm", "warm_eligible" -> WARM_ELIGIBLE;
            case "cold-only", "cold", "cold_only"         -> COLD_ONLY;
            default -> throw new IllegalArgumentException("Unknown pod role: " + s);
        };
    }
}
