package io.saga.core.fault;

/**
 * Hook consulted by the orchestrator around every step execution. Used to
 * drive end-to-end failure tests: fail before/after step N, drop a forward
 * delivery, or simulate a process crash.
 */
public interface FaultInjector {

    /** What the engine should do with the step's forward action. */
    enum Decision {
        PROCEED,
        /** Simulate a lost forward delivery: step is recorded SUCCEEDED without running the action. */
        SKIP_ACTION
    }

    /** Called immediately before the forward action of a step runs. */
    default Decision beforeStep(StepPoint point) throws Exception {
        return Decision.PROCEED;
    }

    /** Called immediately after the forward action of a step succeeded. */
    default void afterStep(StepPoint point) throws Exception {
    }

    FaultInjector NONE = new FaultInjector() { };

    /** Identifies the injection point: one specific step of one saga instance. */
    record StepPoint(String sagaId, int stepIndex, String stepName) { }
}
