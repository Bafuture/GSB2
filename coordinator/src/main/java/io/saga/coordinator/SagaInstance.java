package io.saga.coordinator;

import java.time.Instant;

public record SagaInstance(String sagaId, String name, SagaStatus status, String failReason,
                           Instant createdAt, Instant updatedAt) {
}
