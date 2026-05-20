package com.example.marketdata.leader;

/**
 * Runtime state of this pod. Drives transitions in {@link LeaderElectionService}.
 * See architecture diagram page 7.
 */
public enum LeaderState {
    STARTING,
    COMPETING,
    LEADER,
    WARM_STANDBY,
    COLD_STANDBY,
    DEGRADED,
    TERMINATING
}
