package com.example.saga.coordinator;

/**
 * Simulates a process crash: propagates out of the engine without any further
 * state transition, leaving the saga in its last persisted state for recovery.
 */
public class SagaCrashException extends RuntimeException {
    public SagaCrashException(String message) {
        super(message);
    }
}
