package io.saga.coordinator;

import java.time.Duration;
import java.util.Objects;

/** Bounded exponential-backoff retry policy applied to every step and compensation. */
public record RetryPolicy(int maxAttempts, Duration initialBackoff, double multiplier) {

    public static final RetryPolicy DEFAULT = new RetryPolicy(3, Duration.ofMillis(100), 2.0);

    public RetryPolicy {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be >= 1");
        }
        Objects.requireNonNull(initialBackoff, "initialBackoff");
        if (multiplier < 1.0) {
            throw new IllegalArgumentException("multiplier must be >= 1.0");
        }
    }

    public static RetryPolicy exponential(int maxAttempts, Duration initialBackoff, double multiplier) {
        return new RetryPolicy(maxAttempts, initialBackoff, multiplier);
    }

    public static RetryPolicy noRetry() {
        return new RetryPolicy(1, Duration.ZERO, 1.0);
    }

    public Duration backoffFor(int failedAttempts) {
        double millis = initialBackoff.toMillis() * Math.pow(multiplier, failedAttempts - 1);
        return Duration.ofMillis((long) Math.min(millis, 60_000));
    }
}
