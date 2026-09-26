package com.example.saga.core;

import java.time.Duration;
import java.util.Objects;

/** A single saga step: a forward action plus its idempotent compensation. */
public final class SagaStep {

    private final String name;
    private final SagaAction action;
    private final SagaCompensation compensation;
    private Duration timeout = Duration.ofSeconds(5);
    private RetryPolicy retryPolicy = RetryPolicy.DEFAULT;

    public SagaStep(String name, SagaAction action, SagaCompensation compensation) {
        this.name = Objects.requireNonNull(name, "name");
        this.action = Objects.requireNonNull(action, "action");
        this.compensation = Objects.requireNonNull(compensation, "compensation");
    }

    public static SagaStep of(String name, SagaAction action, SagaCompensation compensation) {
        return new SagaStep(name, action, compensation);
    }

    public SagaStep withTimeout(Duration timeout) {
        this.timeout = Objects.requireNonNull(timeout);
        return this;
    }

    public SagaStep withRetry(RetryPolicy retryPolicy) {
        this.retryPolicy = Objects.requireNonNull(retryPolicy);
        return this;
    }

    public String getName() {
        return name;
    }

    public SagaAction getAction() {
        return action;
    }

    public SagaCompensation getCompensation() {
        return compensation;
    }

    public Duration getTimeout() {
        return timeout;
    }

    public RetryPolicy getRetryPolicy() {
        return retryPolicy;
    }
}
