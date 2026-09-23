package io.saga.core;

import java.time.Duration;
import java.util.Objects;

/** Bounded retries with exponential backoff. */
public final class RetryPolicy {

    private final int maxAttempts;
    private final Duration initialBackoff;
    private final double multiplier;
    private final Duration maxBackoff;

    private RetryPolicy(int maxAttempts, Duration initialBackoff, double multiplier, Duration maxBackoff) {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be >= 1");
        }
        this.maxAttempts = maxAttempts;
        this.initialBackoff = Objects.requireNonNull(initialBackoff);
        this.multiplier = multiplier;
        this.maxBackoff = Objects.requireNonNull(maxBackoff);
    }

    public static RetryPolicy of(int maxAttempts, Duration initialBackoff, double multiplier) {
        return new RetryPolicy(maxAttempts, initialBackoff, multiplier, Duration.ofSeconds(30));
    }

    public static RetryPolicy none() {
        return new RetryPolicy(1, Duration.ZERO, 1.0, Duration.ZERO);
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    /** Delay to sleep before attempt {@code failedAttempts + 1} (1-based failure count). */
    public Duration backoffForFailure(int failedAttempts) {
        double millis = initialBackoff.toMillis() * Math.pow(multiplier, Math.max(0, failedAttempts - 1));
        long capped = (long) Math.min(millis, maxBackoff.toMillis());
        return Duration.ofMillis(capped);
    }
}
