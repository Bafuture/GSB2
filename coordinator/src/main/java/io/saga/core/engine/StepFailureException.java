package io.saga.core.engine;

/** A forward step exhausted its retries (or hit a deterministic fault); triggers compensation. */
class StepFailureException extends RuntimeException {
    StepFailureException(String message) {
        super(message);
    }
}
