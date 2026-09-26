package com.example.saga.participant;

import javax.sql.DataSource;

/**
 * Participant-side barrier guarding a single saga branch (one step of one saga instance).
 *
 * <p>It provides the three classic protections of a saga/TCC-style participant:</p>
 * <ul>
 *   <li><b>Idempotency</b> — the same forward or compensation request delivered multiple
 *       times is executed at most once (duplicate requests are skipped).</li>
 *   <li><b>Null rollback</b> — a compensation request for a branch whose forward action
 *       never executed is acknowledged without running the compensation logic.</li>
 *   <li><b>Suspension</b> — if a compensation request arrived before the forward action
 *       (out-of-order delivery), the late forward action is rejected so the branch does
 *       not "hang" in a state that can never be compensated.</li>
 * </ul>
 *
 * <p>All decisions are driven by barrier rows persisted in the database, so the
 * protection survives process restarts.</p>
 */
public class ParticipantBarrier {

    private final BarrierRepository repository;

    public ParticipantBarrier(DataSource dataSource) {
        this.repository = new BarrierRepository(dataSource);
    }

    public ParticipantBarrier(BarrierRepository repository) {
        this.repository = repository;
    }

    /**
     * Guards the forward (try) action of a branch.
     *
     * @return {@link BarrierResult#EXECUTED} if the action ran now,
     *         {@link BarrierResult#DUPLICATE_SKIPPED} if it already ran before,
     *         {@link BarrierResult#SUSPENDED} if a compensation beat us to it
     */
    public BarrierResult tryExecute(String sagaId, String stepName, ParticipantAction action) throws Exception {
        if (repository.exists(sagaId, stepName, BarrierType.COMPENSATE)) {
            // Suspension: compensation already recorded -> reject the late forward action.
            return BarrierResult.SUSPENDED;
        }
        if (!repository.insertIfAbsent(sagaId, stepName, BarrierType.TRY)) {
            // Duplicate delivery of the forward request.
            return BarrierResult.DUPLICATE_SKIPPED;
        }
        try {
            action.execute();
            return BarrierResult.EXECUTED;
        } catch (Exception | Error e) {
            // Allow retries to re-enter: the branch is not considered executed.
            repository.delete(sagaId, stepName, BarrierType.TRY);
            throw e;
        }
    }

    /**
     * Guards the compensation action of a branch.
     *
     * @return {@link BarrierResult#EXECUTED} if the compensation ran now,
     *         {@link BarrierResult#DUPLICATE_SKIPPED} if it already ran before (idempotent),
     *         {@link BarrierResult#NULL_ROLLBACK} if the forward action never ran
     */
    public BarrierResult compensate(String sagaId, String stepName, ParticipantAction compensation) throws Exception {
        if (!repository.insertIfAbsent(sagaId, stepName, BarrierType.COMPENSATE)) {
            // Compensation already handled -> idempotent skip.
            return BarrierResult.DUPLICATE_SKIPPED;
        }
        if (!repository.exists(sagaId, stepName, BarrierType.TRY)) {
            // Null rollback: nothing was ever done, so nothing must be undone.
            return BarrierResult.NULL_ROLLBACK;
        }
        try {
            compensation.execute();
            return BarrierResult.EXECUTED;
        } catch (Exception | Error e) {
            // Allow retries of a failed compensation.
            repository.delete(sagaId, stepName, BarrierType.COMPENSATE);
            throw e;
        }
    }

    public boolean tryExecuted(String sagaId, String stepName) {
        return repository.exists(sagaId, stepName, BarrierType.TRY);
    }
}
