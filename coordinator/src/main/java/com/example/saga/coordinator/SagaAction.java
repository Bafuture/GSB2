package com.example.saga.coordinator;

@FunctionalInterface
public interface SagaAction {
    void execute(SagaContext context) throws Exception;
}
