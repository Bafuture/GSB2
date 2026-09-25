package io.saga.coordinator;

/** Thrown when a step has exhausted its retry budget; triggers compensation. */
public final class StepFailedException extends Exception {
    private final String stepName;

    public StepFailedException(String stepName, String reason) {
        super("step '" + stepName + "' failed: " + reason);
        this.stepName = stepName;
    }

    public String stepName() {
        return stepName;
    }
}
