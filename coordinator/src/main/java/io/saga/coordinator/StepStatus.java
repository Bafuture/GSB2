package io.saga.coordinator;

public enum StepStatus {
    PENDING,
    RUNNING,
    SUCCEEDED,
    FAILED,
    COMPENSATED
}
