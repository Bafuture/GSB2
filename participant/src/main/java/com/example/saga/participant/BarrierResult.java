package com.example.saga.participant;

public enum BarrierResult {
    /** Forward action executed and barrier recorded. */
    FORWARD_EXECUTED,
    /** Duplicate forward request; previous execution kept, action not re-run. */
    FORWARD_DUPLICATE,
    /** Compensation arrived before the forward request (suspension); forward rejected. */
    FORWARD_SUSPENDED,
    /** Compensation executed and barrier recorded. */
    COMPENSATED,
    /** Compensation requested but the forward never executed (null rollback); recorded as a no-op. */
    COMPENSATE_NULL_ROLLBACK,
    /** Duplicate compensation request; idempotent no-op. */
    COMPENSATE_DUPLICATE
}
