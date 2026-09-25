package io.saga.coordinator;

/**
 * Simulates a hard process crash. The orchestrator lets it escape without
 * touching persisted state, leaving the saga exactly as a real crash would.
 */
public final class CrashException extends RuntimeException {
    public CrashException(String message) {
        super(message);
    }
}
