package io.saga.core.fault;

/** Deterministic fault raised by {@link ConfigurableFaultInjector}; never retried. */
public class InjectedFaultException extends RuntimeException {
    public InjectedFaultException(String message) {
        super(message);
    }
}
