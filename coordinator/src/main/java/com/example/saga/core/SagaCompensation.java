package com.example.saga.core;

/** The compensation (undo) action of a saga step. Must be idempotent. */
@FunctionalInterface
public interface SagaCompensation {
    void compensate(SagaContext context) throws Exception;
}
