package io.saga.coordinator;

import java.time.Instant;

/** One entry of a saga's persisted event timeline. */
public record SagaEvent(long id, String sagaId, Instant timestamp, String type, String message) {
}
