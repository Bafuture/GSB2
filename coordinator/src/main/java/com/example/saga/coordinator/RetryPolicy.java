package com.example.saga.coordinator;

import java.time.Duration;

public record RetryPolicy(int maxAttempts, Duration initialBackoff, double backoffMultiplier) {

    public static RetryPolicy none() {
        return new RetryPolicy(1, Duration.ZERO, 1.0);
    }

    public static RetryPolicy of(int maxAttempts, Duration initialBackoff, double backoffMultiplier) {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be >= 1");
        }
        return new RetryPolicy(maxAttempts, initialBackoff, backoffMultiplier);
    }

    public static RetryPolicy defaultPolicy() {
        return new RetryPolicy(3, Duration.ofMillis(50), 2.0);
    }

    long backoffMillisForAttempt(int attempt) {
        return (long) (initialBackoff.toMillis() * Math.pow(backoffMultiplier, attempt - 1));
    }
}
