package io.saga.core;

/** Lifecycle of a single step inside a saga instance. */
public enum StepStatus {
    PENDING,
    RUNNING,
    SUCCEEDED,
    FAILED,
    COMPENSATING,
    COMPENSATED,
    COMPENSATION_FAILED
}
