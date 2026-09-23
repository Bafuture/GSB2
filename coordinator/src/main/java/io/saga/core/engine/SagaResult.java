package io.saga.core.engine;

import io.saga.core.SagaEvent;
import io.saga.core.SagaStatus;

import java.util.List;

/** Terminal snapshot of a saga run: final status, recorded reason, full event timeline. */
public record SagaResult(String sagaId, SagaStatus status, String failureReason, List<SagaEvent> timeline) {

    public boolean isCompleted() {
        return status == SagaStatus.COMPLETED;
    }
}
