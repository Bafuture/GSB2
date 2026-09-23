package io.saga.core;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Shared, thread-safe data bag for one saga instance. The engine also exposes
 * the currently executing step through a thread-local so that participants can
 * derive a stable idempotency key ({code sagaId + ":" + stepName}) even when
 * steps of a parallel group run on different threads.
 */
public class SagaContext {

    private static final ThreadLocal<String> CURRENT_STEP = new ThreadLocal<>();

    private final String sagaId;
    private final Map<String, Object> data = new ConcurrentHashMap<>();

    public SagaContext(String sagaId) {
        this.sagaId = sagaId;
    }

    public String getSagaId() {
        return sagaId;
    }

    public SagaContext put(String key, Object value) {
        data.put(key, value);
        return this;
    }

    public Optional<Object> get(String key) {
        return Optional.ofNullable(data.get(key));
    }

    @SuppressWarnings("unchecked")
    public <T> T get(String key, Class<T> type) {
        return (T) data.get(key);
    }

    /** Idempotency key for the action currently running on this thread. */
    public String idempotencyKey() {
        String step = CURRENT_STEP.get();
        return sagaId + ":" + (step == null ? "unknown" : step);
    }

    public static void setCurrentStep(String stepName) {
        CURRENT_STEP.set(stepName);
    }

    public static void clearCurrentStep() {
        CURRENT_STEP.remove();
    }
}
