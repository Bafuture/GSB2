package io.saga.core;

/** A single unit of work (forward action or compensation) inside a saga step. */
@FunctionalInterface
public interface SagaAction {
    void execute(SagaContext context) throws Exception;
}
