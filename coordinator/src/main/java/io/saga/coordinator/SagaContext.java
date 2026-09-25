package io.saga.coordinator;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** Mutable bag of attributes shared by all steps of one saga instance. */
public final class SagaContext {
    private final String sagaId;
    private final Map<String, Object> attributes = new ConcurrentHashMap<>();

    public SagaContext(String sagaId) {
        this.sagaId = sagaId;
    }

    public String sagaId() {
        return sagaId;
    }

    public SagaContext put(String key, Object value) {
        attributes.put(key, value);
        return this;
    }

    @SuppressWarnings("unchecked")
    public <T> Optional<T> get(String key) {
        return Optional.ofNullable((T) attributes.get(key));
    }
}
