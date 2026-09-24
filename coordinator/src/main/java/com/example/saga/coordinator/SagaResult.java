package com.example.saga.coordinator;

import java.util.List;

public record SagaResult(String sagaId, SagaStatus status, String failureReason, List<SagaEvent> timeline) {

    public boolean isCompleted() {
        return status == SagaStatus.COMPLETED;
    }
}
