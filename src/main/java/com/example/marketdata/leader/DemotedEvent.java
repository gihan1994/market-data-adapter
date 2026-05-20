package com.example.marketdata.leader;

import org.springframework.context.ApplicationEvent;

/**
 * Published when this pod loses the leader lock (lock expired, voluntarily released,
 * or pod is shutting down).
 */
public class DemotedEvent extends ApplicationEvent {
    private final String podName;
    private final String reason;
    public DemotedEvent(Object source, String podName, String reason) {
        super(source);
        this.podName = podName;
        this.reason = reason;
    }
    public String getPodName() { return podName; }
    public String getReason()  { return reason; }
}
