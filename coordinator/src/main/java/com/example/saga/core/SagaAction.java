package com.example.saga.core;

/** The forward action of a saga step. */
@FunctionalInterface
public interface SagaAction {
    void execute(SagaContext context) throws Exception;
}
