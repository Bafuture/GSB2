package com.example.saga.participant;

/** Type of a participant barrier record. */
public enum BarrierType {
    /** The forward (try) action of a branch has been invoked. */
    TRY,
    /** A compensation request for a branch has been received. */
    COMPENSATE
}
