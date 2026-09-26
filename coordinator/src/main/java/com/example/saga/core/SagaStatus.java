package com.example.saga.core;

/** Lifecycle states of a saga instance. */
public enum SagaStatus {
    /** Forward steps are being executed. */
    RUNNING,
    /** A forward step failed permanently; compensations are being executed in reverse order. */
    COMPENSATING,
    /** All forward steps succeeded. */
    COMPLETED,
    /** The saga failed and all executed steps were successfully compensated. */
    COMPENSATED,
    /** The saga failed and at least one compensation could not be completed. Manual intervention required. */
    FAILED
}
