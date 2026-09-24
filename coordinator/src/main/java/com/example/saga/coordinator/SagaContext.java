package com.example.saga.coordinator;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SagaContext {
    private final String sagaId;
    private final String sagaName;
    private final ThreadLocal<String> currentStep = new ThreadLocal<>();
    private final Map<String, Object> attributes = new ConcurrentHashMap<>();

    public SagaContext(String sagaId, String sagaName) {
        this.sagaId = sagaId;
        this.sagaName = sagaName;
    }

    public String sagaId() {
        return sagaId;
    }

    public String sagaName() {
        return sagaName;
    }

    public String stepName() {
        return currentStep.get();
    }

    void enterStep(String stepName) {
        currentStep.set(stepName);
    }

    void leaveStep() {
        currentStep.remove();
    }

    public SagaContext put(String key, Object value) {
        attributes.put(key, value);
        return this;
    }

    @SuppressWarnings("unchecked")
    public <T> T get(String key) {
        return (T) attributes.get(key);
    }
}
