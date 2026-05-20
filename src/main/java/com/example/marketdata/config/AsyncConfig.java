package com.example.marketdata.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;

@Configuration
public class AsyncConfig {

    /** Used by SubscriptionManager for batch RIC resubscription during failover. */
    @Bean(name = "resubscribeExecutor")
    public Executor resubscribeExecutor() {
        ThreadPoolTaskExecutor ex = new ThreadPoolTaskExecutor();
        ex.setCorePoolSize(4);
        ex.setMaxPoolSize(8);
        ex.setQueueCapacity(2000);
        ex.setThreadNamePrefix("resubscribe-");
        ex.initialize();
        return ex;
    }

    /** Used by the EMA callback handler if work needs to be off-loaded from the EMA dispatcher. */
    @Bean(name = "tickProcessingExecutor")
    public Executor tickProcessingExecutor() {
        ThreadPoolTaskExecutor ex = new ThreadPoolTaskExecutor();
        ex.setCorePoolSize(2);
        ex.setMaxPoolSize(4);
        ex.setQueueCapacity(10_000);
        ex.setThreadNamePrefix("tick-");
        ex.initialize();
        return ex;
    }
}
