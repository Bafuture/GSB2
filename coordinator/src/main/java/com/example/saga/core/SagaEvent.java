package com.example.saga.core;

import java.time.Instant;

/** One entry of a saga's persisted event timeline. */
public record SagaEvent(String sagaId, String eventType, String detail, Instant timestamp) {
}
