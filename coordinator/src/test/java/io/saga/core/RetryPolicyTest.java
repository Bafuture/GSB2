package io.saga.core;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RetryPolicyTest {

    @Test
    void backoffGrowsExponentiallyFromInitialDelay() {
        RetryPolicy policy = RetryPolicy.of(4, Duration.ofMillis(50), 2.0);
        assertEquals(Duration.ofMillis(50), policy.backoffForFailure(1));
        assertEquals(Duration.ofMillis(100), policy.backoffForFailure(2));
        assertEquals(Duration.ofMillis(200), policy.backoffForFailure(3));
    }

    @Test
    void noRetryPolicyAllowsExactlyOneAttempt() {
        assertEquals(1, RetryPolicy.none().getMaxAttempts());
    }
}
