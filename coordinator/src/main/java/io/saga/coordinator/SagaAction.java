package io.saga.coordinator;

/** A unit of work executed for a saga step (forward action or compensation). */
@FunctionalInterface
public interface SagaAction {
    void run(SagaContext ctx) throws Exception;
}
