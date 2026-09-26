package com.example.saga.core;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;

/** Terminal (or current) view of a saga instance, including its event timeline. */
public record SagaResult(String sagaId,
                         String sagaName,
                         SagaStatus status,
                         String failReason,
                         List<SagaEvent> events) {

    private static final DateTimeFormatter TIME_FORMAT =
            DateTimeFormatter.ofPattern("HH:mm:ss.SSS").withZone(ZoneId.systemDefault());

    public boolean isCompleted() {
        return status == SagaStatus.COMPLETED;
    }

    /** Renders the persisted event timeline, one event per line. */
    public String timeline() {
        StringBuilder sb = new StringBuilder();
        for (SagaEvent event : events) {
            sb.append(TIME_FORMAT.format(event.timestamp()))
                    .append("  ")
                    .append(event.eventType())
                    .append(event.detail() == null || event.detail().isBlank() ? "" : "  " + event.detail())
                    .append(System.lineSeparator());
        }
        return sb.toString();
    }
}
