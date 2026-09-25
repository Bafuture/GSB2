package io.saga.coordinator;

/**
 * Saga lifecycle:
 *   RUNNING -&gt; COMPLETED
 *   RUNNING -&gt; COMPENSATING -&gt; COMPENSATED
 *   COMPENSATING -&gt; FAILED (a compensation could not be applied; manual intervention required)
 *
 * COMPENSATED and FAILED are both failure terminals; the original failure
 * reason is always persisted on the saga row.
 */
public enum SagaStatus {
    RUNNING,
    COMPLETED,
    COMPENSATING,
    COMPENSATED,
    FAILED;

    public boolean isTerminal() {
        return this == COMPLETED || this == COMPENSATED || this == FAILED;
    }
}
