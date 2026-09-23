package io.saga.core;

import java.time.LocalDateTime;

/** One immutable entry of the persisted saga timeline. */
public record SagaEvent(long id, String sagaId, EventType type, String detail, LocalDateTime timestamp) {

    @Override
    public String toString() {
        String base = timestamp + " " + type;
        return detail == null || detail.isBlank() ? base : base + "  " + detail;
    }
}
