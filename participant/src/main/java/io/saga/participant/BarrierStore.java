package io.saga.participant;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Optional;
import javax.sql.DataSource;

/**
 * JDBC-backed storage for participant barriers. The primary key on
 * (tx_id, step_id, branch) makes barrier inserts idempotent even under
 * duplicate delivery.
 */
public final class BarrierStore {

    private final DataSource dataSource;

    public BarrierStore(DataSource dataSource) {
        this.dataSource = dataSource;
        initSchema();
    }

    private void initSchema() {
        try (Connection c = dataSource.getConnection(); Statement s = c.createStatement()) {
            s.execute("""
                    CREATE TABLE IF NOT EXISTS participant_barrier (
                      tx_id      VARCHAR(64) NOT NULL,
                      step_id    VARCHAR(128) NOT NULL,
                      branch     VARCHAR(64) NOT NULL,
                      status     VARCHAR(32) NOT NULL,
                      updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
                      PRIMARY KEY (tx_id, step_id, branch)
                    )""");
        } catch (SQLException e) {
            throw new IllegalStateException("failed to initialise barrier schema", e);
        }
    }

    public synchronized Optional<BranchStatus> find(String txId, String stepId, String branch) {
        String sql = "SELECT status FROM participant_barrier WHERE tx_id=? AND step_id=? AND branch=?";
        try (Connection c = dataSource.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, txId);
            ps.setString(2, stepId);
            ps.setString(3, branch);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return Optional.empty();
                }
                return Optional.of(BranchStatus.valueOf(rs.getString(1)));
            }
        } catch (SQLException e) {
            throw new BarrierPersistenceException(e);
        }
    }

    /** Inserts a barrier row; returns false if one already exists (duplicate delivery). */
    public synchronized boolean insertIfAbsent(String txId, String stepId, String branch, BranchStatus status) {
        String sql = "INSERT INTO participant_barrier (tx_id, step_id, branch, status, updated_at)"
                + " VALUES (?,?,?,?,?)";
        try (Connection c = dataSource.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, txId);
            ps.setString(2, stepId);
            ps.setString(3, branch);
            ps.setString(4, status.name());
            ps.setTimestamp(5, Timestamp.from(Instant.now()));
            ps.executeUpdate();
            return true;
        } catch (SQLException e) {
            if (isDuplicateKey(e)) {
                return false;
            }
            throw new BarrierPersistenceException(e);
        }
    }

    public synchronized void update(String txId, String stepId, String branch, BranchStatus status) {
        String sql = "UPDATE participant_barrier SET status=?, updated_at=? WHERE tx_id=? AND step_id=? AND branch=?";
        try (Connection c = dataSource.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, status.name());
            ps.setTimestamp(2, Timestamp.from(Instant.now()));
            ps.setString(3, txId);
            ps.setString(4, stepId);
            ps.setString(5, branch);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new BarrierPersistenceException(e);
        }
    }

    private static boolean isDuplicateKey(SQLException e) {
        return "23505".equals(e.getSQLState());
    }

    public static final class BarrierPersistenceException extends RuntimeException {
        BarrierPersistenceException(SQLException cause) {
            super(cause);
        }
    }
}
