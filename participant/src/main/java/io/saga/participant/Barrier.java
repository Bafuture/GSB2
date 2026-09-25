package io.saga.participant;

import java.util.Optional;

/**
 * Participant-side barrier protecting against the three classic anomalies of
 * compensating transactions:
 * <ul>
 *   <li><b>Idempotency</b> - a duplicate forward/compensation request is a no-op,
 *       because the barrier row records that the branch already ran.</li>
 *   <li><b>Null rollback</b> - a compensation arriving for a branch whose forward
 *       never ran inserts a COMPENSATED barrier and succeeds without executing
 *       the compensation action.</li>
 *   <li><b>Hanging</b> - a forward action arriving after the branch was already
 *       compensated is rejected, so the compensation is never "undone".</li>
 * </ul>
 */
public final class Barrier {

    private static final String DEFAULT_BRANCH = "default";

    private final BarrierStore store;

    public Barrier(BarrierStore store) {
        this.store = store;
    }

    /**
     * Runs the forward action unless the branch already ran or was compensated.
     *
     * @return true if the action was executed, false if it was suppressed
     *         (duplicate request or hanging forward after compensation)
     */
    public boolean forward(String txId, String stepId, ParticipantAction action) throws Exception {
        return forward(txId, stepId, DEFAULT_BRANCH, action);
    }

    public synchronized boolean forward(String txId, String stepId, String branch,
                                        ParticipantAction action) throws Exception {
        Optional<BranchStatus> current = store.find(txId, stepId, branch);
        if (current.isPresent()) {
            // FORWARD_DONE -> duplicate delivery, skip. COMPENSATED -> hanging forward, reject.
            return false;
        }
        action.run();
        if (!store.insertIfAbsent(txId, stepId, branch, BranchStatus.FORWARD_DONE)) {
            // Lost a race against a concurrent request; the other one won.
            return false;
        }
        return true;
    }

    /**
     * Runs the compensation unless it already ran; records a null-rollback
     * barrier if the forward never executed.
     *
     * @return true if the compensation action was executed, false if it was
     *         suppressed (idempotent repeat or null rollback)
     */
    public boolean compensate(String txId, String stepId, ParticipantAction action) throws Exception {
        return compensate(txId, stepId, DEFAULT_BRANCH, action);
    }

    public synchronized boolean compensate(String txId, String stepId, String branch,
                                           ParticipantAction action) throws Exception {
        Optional<BranchStatus> current = store.find(txId, stepId, branch);
        if (current.isEmpty()) {
            // Null rollback: nothing to undo, but record the barrier so a late
            // forward action (hanging) is rejected afterwards.
            store.insertIfAbsent(txId, stepId, branch, BranchStatus.COMPENSATED);
            return false;
        }
        if (current.get() == BranchStatus.COMPENSATED) {
            return false; // already compensated: idempotent no-op
        }
        action.run();
        store.update(txId, stepId, branch, BranchStatus.COMPENSATED);
        return true;
    }
}
