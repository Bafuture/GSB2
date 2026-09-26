package com.example.saga.participant;

/** Outcome of a barrier-guarded branch invocation. */
public enum BarrierResult {
    /** The guarded action was executed by this call. */
    EXECUTED,
    /** The same branch was already handled before; the call was skipped (idempotency). */
    DUPLICATE_SKIPPED,
    /** A compensation arrived but the forward action never ran; nothing to undo (null rollback). */
    NULL_ROLLBACK,
    /** A compensation arrived before the forward action; the late forward action is rejected (suspension). */
    SUSPENDED
}
