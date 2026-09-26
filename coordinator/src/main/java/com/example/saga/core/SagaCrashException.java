package com.example.saga.core;

/**
 * Simulates a hard process crash. The engine propagates this exception without
 * changing saga/step state, leaving the persisted state exactly as it was at the
 * moment of the "crash" so that {@code recover()} can resume it.
 */
public class SagaCrashException extends RuntimeException {
    public SagaCrashException(String message) {
        super(message);
    }
}
