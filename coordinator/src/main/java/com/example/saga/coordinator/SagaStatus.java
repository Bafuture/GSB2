package com.example.saga.coordinator;

public enum SagaStatus {
    RUNNING,
    COMPLETED,
    COMPENSATING,
    COMPENSATED,
    FAILED
}
