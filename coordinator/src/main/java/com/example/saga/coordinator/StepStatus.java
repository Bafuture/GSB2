package com.example.saga.coordinator;

public enum StepStatus {
    PENDING,
    RUNNING,
    SUCCEEDED,
    FAILED,
    COMPENSATED
}
