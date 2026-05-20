package com.example.marketdata.subscription;

import com.example.marketdata.config.MarketDataProperties;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RAtomicLong;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Service;

/**
 * Per-RIC reference count stored in Redis. Used to decide whether to actually call
 * EMA subscribe / unsubscribe. The MS-level subscription table (PostgreSQL) is the
 * durable source of truth; Redis is the fast path.
 */
@Service
@Slf4j
public class RefcountService {

    private final RedissonClient redisson;
    private final MarketDataProperties props;

    public RefcountService(RedissonClient redisson, MarketDataProperties props) {
        this.redisson = redisson;
        this.props = props;
    }

    public long increment(String ric) {
        long v = counter(ric).incrementAndGet();
        log.debug("refcount[{}] ++ -> {}", ric, v);
        return v;
    }

    public long decrement(String ric) {
        long v = counter(ric).decrementAndGet();
        if (v < 0) {
            log.warn("refcount[{}] went negative ({}), resetting to 0", ric, v);
            counter(ric).set(0);
            return 0;
        }
        log.debug("refcount[{}] -- -> {}", ric, v);
        return v;
    }

    public long get(String ric) {
        return counter(ric).get();
    }

    public void reset(String ric) {
        counter(ric).delete();
    }

    private RAtomicLong counter(String ric) {
        return redisson.getAtomicLong(props.getSubscription().getRefcountKeyPrefix() + ric);
    }
}
