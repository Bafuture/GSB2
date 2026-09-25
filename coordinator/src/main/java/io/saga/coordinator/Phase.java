package io.saga.coordinator;

/** One element of a saga definition: either a single step or a parallel group of steps. */
public sealed interface Phase permits SagaStep, ParallelGroup {
    String name();
}
