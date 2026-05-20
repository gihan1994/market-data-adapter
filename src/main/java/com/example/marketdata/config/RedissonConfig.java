package com.example.marketdata.config;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

@Configuration
public class RedissonConfig {

    @Value("${redis.host:localhost}")    private String host;
    @Value("${redis.port:6379}")          private int port;
    @Value("${redis.password:}")          private String password;
    @Value("${redis.cluster-enabled:false}") private boolean clusterEnabled;
    @Value("${redis.cluster-nodes:}")     private String clusterNodes;

    /**
     * Redisson client. Used for:
     *  - distributed lock (leader election)
     *  - distributed counters (RIC refcount)
     *  - regular Redis commands via RedissonClient.getBucket / getMap
     */
    @Bean(destroyMethod = "shutdown")
    public RedissonClient redissonClient() {
        Config config = new Config();
        config.setThreads(8);
        config.setNettyThreads(8);

        if (clusterEnabled) {
            String[] nodes = clusterNodes.split(",");
            for (int i = 0; i < nodes.length; i++) {
                nodes[i] = ensureProtocol(nodes[i].trim());
            }
            var cluster = config.useClusterServers()
                    .addNodeAddress(nodes)
                    .setScanInterval(2000)
                    .setReadMode(org.redisson.config.ReadMode.MASTER_SLAVE);
            if (StringUtils.hasText(password)) cluster.setPassword(password);
        } else {
            var single = config.useSingleServer()
                    .setAddress(ensureProtocol(host + ":" + port))
                    .setConnectionPoolSize(32)
                    .setConnectionMinimumIdleSize(8);
            if (StringUtils.hasText(password)) single.setPassword(password);
        }

        return Redisson.create(config);
    }

    private static String ensureProtocol(String node) {
        return node.startsWith("redis://") || node.startsWith("rediss://")
                ? node : "redis://" + node;
    }
}
