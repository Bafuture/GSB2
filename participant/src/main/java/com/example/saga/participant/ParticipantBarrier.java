package com.example.saga.participant;

import com.example.saga.coordinator.SagaAction;
import com.example.saga.coordinator.SagaContext;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.function.Consumer;

/**
 * Participant-side barrier guarding against the classic distributed-transaction
 * anomalies, backed by a unique-keyed barrier table:
 *
 * <ul>
 *   <li><b>Idempotency</b>: a second forward/compensate request for the same
 *       (saga, step) is a no-op.</li>
 *   <li><b>Null rollback</b>: a compensate request without a prior forward is
 *       recorded and answered successfully without running the compensation.</li>
 *   <li><b>Suspension</b>: a forward request arriving after a compensate barrier
 *       was recorded is rejected and the action is not executed.</li>
 * </ul>
 */
public class ParticipantBarrier {

    private static final String PHASE_FORWARD = "FORWARD";
    private static final String PHASE_COMPENSATE = "COMPENSATE";

    private final DataSource dataSource;
    private final String participant;
    private volatile Consumer<String> logger = message -> { };

    public ParticipantBarrier(DataSource dataSource, String participant) {
        this.dataSource = dataSource;
        this.participant = participant;
        initSchema();
    }

    public void setLogger(Consumer<String> logger) {
        this.logger = logger;
    }

    private void initSchema() {
        try (Connection c = dataSource.getConnection(); Statement s = c.createStatement()) {
            s.execute("""
                    CREATE TABLE IF NOT EXISTS participant_barrier (
                      participant VARCHAR(64)  NOT NULL,
                      saga_id     VARCHAR(64)  NOT NULL,
                      step_name   VARCHAR(128) NOT NULL,
                      phase       VARCHAR(16)  NOT NULL,
                      created_at  TIMESTAMP    NOT NULL,
                      PRIMARY KEY (participant, saga_id, step_name, phase)
                    )
                    """);
        } catch (SQLException e) {
            throw new IllegalStateException("failed to initialize barrier schema", e);
        }
    }

    public BarrierResult tryForward(String sagaId, String stepName, SagaAction action, SagaContext ctx) throws Exception {
        if (barrierExists(sagaId, stepName, PHASE_COMPENSATE)) {
            log(sagaId, stepName, "SUSPENDED: forward rejected, compensation already recorded");
            return BarrierResult.FORWARD_SUSPENDED;
        }
        if (barrierExists(sagaId, stepName, PHASE_FORWARD)) {
            log(sagaId, stepName, "DUPLICATE: forward already executed, skipping");
            return BarrierResult.FORWARD_DUPLICATE;
        }
        try {
            insertBarrier(sagaId, stepName, PHASE_FORWARD);
        } catch (SQLIntegrityConstraintViolationException duplicate) {
            log(sagaId, stepName, "DUPLICATE: forward already executed, skipping");
            return BarrierResult.FORWARD_DUPLICATE;
        }
        try {
            action.execute(ctx);
            log(sagaId, stepName, "FORWARD executed");
            return BarrierResult.FORWARD_EXECUTED;
        } catch (Exception e) {
            deleteBarrier(sagaId, stepName, PHASE_FORWARD);
            throw e;
        }
    }

    public BarrierResult tryCompensate(String sagaId, String stepName, SagaAction action, SagaContext ctx) throws Exception {
        if (barrierExists(sagaId, stepName, PHASE_COMPENSATE)) {
            log(sagaId, stepName, "DUPLICATE: compensation already applied, skipping");
            return BarrierResult.COMPENSATE_DUPLICATE;
        }
        if (!barrierExists(sagaId, stepName, PHASE_FORWARD)) {
            insertBarrier(sagaId, stepName, PHASE_COMPENSATE);
            log(sagaId, stepName, "NULL ROLLBACK: no forward recorded, compensation is a no-op");
            return BarrierResult.COMPENSATE_NULL_ROLLBACK;
        }
        action.execute(ctx);
        try {
            insertBarrier(sagaId, stepName, PHASE_COMPENSATE);
        } catch (SQLIntegrityConstraintViolationException duplicate) {
            log(sagaId, stepName, "DUPLICATE: compensation already applied, skipping");
            return BarrierResult.COMPENSATE_DUPLICATE;
        }
        log(sagaId, stepName, "COMPENSATION executed");
        return BarrierResult.COMPENSATED;
    }

    private boolean barrierExists(String sagaId, String stepName, String phase) {
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(
                     "SELECT 1 FROM participant_barrier WHERE participant=? AND saga_id=? AND step_name=? AND phase=?")) {
            ps.setString(1, participant);
            ps.setString(2, sagaId);
            ps.setString(3, stepName);
            ps.setString(4, phase);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        } catch (SQLException e) {
            throw new IllegalStateException("barrier lookup failed", e);
        }
    }

    private void insertBarrier(String sagaId, String stepName, String phase) throws SQLIntegrityConstraintViolationException {
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(
                     "INSERT INTO participant_barrier (participant, saga_id, step_name, phase, created_at) VALUES (?,?,?,?,?)")) {
            ps.setString(1, participant);
            ps.setString(2, sagaId);
            ps.setString(3, stepName);
            ps.setString(4, phase);
            ps.setTimestamp(5, Timestamp.from(Instant.now()));
            ps.executeUpdate();
        } catch (SQLIntegrityConstraintViolationException e) {
            throw e;
        } catch (SQLException e) {
            throw new IllegalStateException("barrier insert failed", e);
        }
    }

    private void deleteBarrier(String sagaId, String stepName, String phase) {
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(
                     "DELETE FROM participant_barrier WHERE participant=? AND saga_id=? AND step_name=? AND phase=?")) {
            ps.setString(1, participant);
            ps.setString(2, sagaId);
            ps.setString(3, stepName);
            ps.setString(4, phase);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("barrier delete failed", e);
        }
    }

    private void log(String sagaId, String stepName, String message) {
        logger.accept("[participant=" + participant + " saga=" + sagaId + " step=" + stepName + "] " + message);
    }
}
