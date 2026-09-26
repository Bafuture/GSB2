package com.example.saga.core;

/** A regular injected fault: treated by the engine as a step failure (retryable). */
public class FaultInjectedException extends RuntimeException {
    public FaultInjectedException(String message) {
        super(message);
    }
}
