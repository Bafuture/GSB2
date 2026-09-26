package com.example.saga.core;

/** Lifecycle states of a single step instance. */
public enum StepStatus {
    PENDING,
    RUNNING,
    SUCCEEDED,
    FAILED,
    COMPENSATED
}
