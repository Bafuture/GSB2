package io.saga.coordinator;

/** Entry point of the saga definition DSL. */
public final class Saga {
    private Saga() {
    }

    public static SagaBuilder define(String name) {
        return new SagaBuilder(name);
    }
}
