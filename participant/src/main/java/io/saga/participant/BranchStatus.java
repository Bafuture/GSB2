package io.saga.participant;

/** Persisted barrier state of one (transaction, step) branch. */
public enum BranchStatus {
    /** The forward action completed successfully. */
    FORWARD_DONE,
    /** The branch is compensated (possibly without the forward ever running: null rollback). */
    COMPENSATED
}
