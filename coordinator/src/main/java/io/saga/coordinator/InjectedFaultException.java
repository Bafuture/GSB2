package io.saga.coordinator;

/** A deterministic fault raised by the {@link FaultInjector}; never retried, goes straight to compensation. */
public final class InjectedFaultException extends RuntimeException {
    public InjectedFaultException(String message) {
        super(message);
    }
}
