package com.example.saga.core;

/**
 * Bounded retry with exponential backoff.
 *
 * @param maxAttempts         total number of attempts (1 = no retry)
 * @param initialBackoffMillis backoff before the first retry
 * @param backoffMultiplier   multiplier applied per subsequent retry
 */
public record RetryPolicy(int maxAttempts, long initialBackoffMillis, double backoffMultiplier) {

    public static final RetryPolicy DEFAULT = new RetryPolicy(3, 50, 2.0);
    public static final RetryPolicy NO_RETRY = new RetryPolicy(1, 0, 1.0);

    public RetryPolicy {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be >= 1");
        }
        if (initialBackoffMillis < 0) {
            throw new IllegalArgumentException("initialBackoffMillis must be >= 0");
        }
    }

    /** Backoff to sleep before the given (1-based) retry attempt. */
    public long backoffBeforeAttempt(int attempt) {
        return (long) (initialBackoffMillis * Math.pow(backoffMultiplier, attempt - 2));
    }
}
