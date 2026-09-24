package com.example.saga.coordinator;

/** Deterministic fault injected by {@link FaultInjector}; fails the step immediately without retries. */
public class SagaFaultException extends RuntimeException {
    public SagaFaultException(String message) {
        super(message);
    }
}
