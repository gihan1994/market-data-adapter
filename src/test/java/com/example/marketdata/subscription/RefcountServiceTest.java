package com.example.marketdata.subscription;

import com.example.marketdata.AbstractIntegrationTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static org.assertj.core.api.Assertions.assertThat;

class RefcountServiceTest extends AbstractIntegrationTest {

    @Autowired RefcountService refcount;

    @Test
    void incrementAndDecrementTrackPerRicCount() {
        String ric = "TEST-INCR-" + System.nanoTime();
        assertThat(refcount.get(ric)).isZero();

        assertThat(refcount.increment(ric)).isEqualTo(1);
        assertThat(refcount.increment(ric)).isEqualTo(2);
        assertThat(refcount.get(ric)).isEqualTo(2);

        assertThat(refcount.decrement(ric)).isEqualTo(1);
        assertThat(refcount.decrement(ric)).isEqualTo(0);

        // does not go negative
        assertThat(refcount.decrement(ric)).isEqualTo(0);

        refcount.reset(ric);
        assertThat(refcount.get(ric)).isZero();
    }
}
