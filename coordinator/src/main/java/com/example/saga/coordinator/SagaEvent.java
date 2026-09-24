package com.example.saga.coordinator;

import java.time.Instant;

public record SagaEvent(long id, String sagaId, String type, String stepName, String message, Instant occurredAt) {
    @Override
    public String toString() {
        String step = stepName == null ? "-" : stepName;
        return occurredAt + "  " + type + "  step=" + step + (message == null ? "" : "  " + message);
    }
}
