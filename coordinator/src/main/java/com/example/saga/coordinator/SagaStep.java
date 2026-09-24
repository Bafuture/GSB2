package com.example.saga.coordinator;

import java.time.Duration;
import java.util.Objects;

public class SagaStep {
    public static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(5);

    private final String name;
    private final SagaAction forward;
    private final SagaAction compensation;
    private final Duration timeout;
    private final RetryPolicy retryPolicy;

    public SagaStep(String name, SagaAction forward, SagaAction compensation) {
        this(name, forward, compensation, DEFAULT_TIMEOUT, RetryPolicy.defaultPolicy());
    }

    public SagaStep(String name, SagaAction forward, SagaAction compensation,
                    Duration timeout, RetryPolicy retryPolicy) {
        this.name = Objects.requireNonNull(name, "name");
        this.forward = Objects.requireNonNull(forward, "forward");
        this.compensation = Objects.requireNonNull(compensation, "compensation");
        this.timeout = Objects.requireNonNull(timeout, "timeout");
        this.retryPolicy = Objects.requireNonNull(retryPolicy, "retryPolicy");
    }

    public String name() {
        return name;
    }

    public SagaAction forward() {
        return forward;
    }

    public SagaAction compensation() {
        return compensation;
    }

    public Duration timeout() {
        return timeout;
    }

    public RetryPolicy retryPolicy() {
        return retryPolicy;
    }
}
