package io.saga.coordinator;

import java.time.Instant;

public record StepRecord(String sagaId, String stepName, int seq, String groupName,
                         StepStatus status, int attempts, String error, Instant updatedAt) {
}
