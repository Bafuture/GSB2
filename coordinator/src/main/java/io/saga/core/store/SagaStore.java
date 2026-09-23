package io.saga.core.store;

import io.saga.core.EventType;
import io.saga.core.SagaEvent;
import io.saga.core.SagaStatus;
import io.saga.core.StepStatus;

import java.util.List;
import java.util.Optional;

/** Persistence contract for saga state. Every step transition and event is durable. */
public interface SagaStore {

    record SagaRecord(String sagaId, String sagaName, SagaStatus status, String failureReason) { }

    record StepRecord(int index, String name, String groupId, StepStatus status, int attempts) { }

    void createSaga(String sagaId, String sagaName);

    void updateSagaStatus(String sagaId, SagaStatus status, String failureReason);

    Optional<SagaRecord> loadSaga(String sagaId);

    /** Sagas that a crashed process may have left behind: RUNNING or COMPENSATING. */
    List<SagaRecord> loadIncomplete();

    void insertStep(String sagaId, int index, String name, String groupId);

    /** Atomic compare-and-set; the basis of idempotent compensation. Returns rows updated. */
    boolean casStepStatus(String sagaId, int index, StepStatus expected, StepStatus target);

    void forceStepStatus(String sagaId, int index, StepStatus target);

    void setStepAttempts(String sagaId, int index, int attempts);

    List<StepRecord> loadSteps(String sagaId);

    void appendEvent(String sagaId, EventType type, String detail);

    List<SagaEvent> loadEvents(String sagaId);
}
