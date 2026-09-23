package io.saga.core;

/** Lifecycle of a saga instance. COMPLETED / COMPENSATED / FAILED are terminal. */
public enum SagaStatus {
    RUNNING,
    COMPENSATING,
    COMPLETED,
    COMPENSATED,
    FAILED
}
