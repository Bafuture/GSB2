package io.saga.core.store;

import io.saga.core.EventType;
import io.saga.core.SagaEvent;
import io.saga.core.SagaStatus;
import io.saga.core.StepStatus;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * H2-backed store (embedded, no external database). Methods are synchronized:
 * H2's MVCC handles the file-level concurrency, the monitor keeps the parallel
 * group's writers from interleaving on a single logical transition.
 */
public class JdbcSagaStore implements SagaStore {

    private static final String DDL = """
        CREATE TABLE IF NOT EXISTS saga_instance (
            saga_id        VARCHAR(64) PRIMARY KEY,
            saga_name      VARCHAR(128) NOT NULL,
            status         VARCHAR(32)  NOT NULL,
            failure_reason CLOB,
            created_at     TIMESTAMP    NOT NULL,
            updated_at     TIMESTAMP    NOT NULL
        );
        CREATE TABLE IF NOT EXISTS saga_step (
            saga_id    VARCHAR(64)  NOT NULL,
            step_idx   INT          NOT NULL,
            step_name  VARCHAR(128) NOT NULL,
            group_id   VARCHAR(64),
            status     VARCHAR(32)  NOT NULL,
            attempts   INT          NOT NULL DEFAULT 0,
            updated_at TIMESTAMP    NOT NULL,
            PRIMARY KEY (saga_id, step_idx)
        );
        CREATE TABLE IF NOT EXISTS saga_event (
            id         BIGINT AUTO_INCREMENT PRIMARY KEY,
            saga_id    VARCHAR(64) NOT NULL,
            event_type VARCHAR(64) NOT NULL,
            detail     VARCHAR(2048),
            created_at TIMESTAMP   NOT NULL
        );
        """;

    private final String jdbcUrl;

    public JdbcSagaStore(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
        initSchema();
    }

    /** In-memory database that survives connection churn for the JVM lifetime. */
    public static JdbcSagaStore inMemory(String name) {
        return new JdbcSagaStore("jdbc:h2:mem:" + name + ";DB_CLOSE_DELAY=-1");
    }

    /** File-based database; two stores on the same path simulate a process restart. */
    public static JdbcSagaStore file(Path directory, String name) {
        return new JdbcSagaStore("jdbc:h2:file:" + directory.resolve(name).toAbsolutePath());
    }

    private void initSchema() {
        try (Connection c = connection(); Statement s = c.createStatement()) {
            for (String stmt : DDL.split(";")) {
                if (!stmt.isBlank()) {
                    s.execute(stmt);
                }
            }
        } catch (SQLException e) {
            throw new StoreException("failed to initialize saga schema", e);
        }
    }

    private Connection connection() throws SQLException {
        return DriverManager.getConnection(jdbcUrl);
    }

    @Override
    public synchronized void createSaga(String sagaId, String sagaName) {
        String sql = "INSERT INTO saga_instance (saga_id, saga_name, status, failure_reason, created_at, updated_at)"
                + " VALUES (?, ?, ?, NULL, ?, ?)";
        try (Connection c = connection(); PreparedStatement ps = c.prepareStatement(sql)) {
            LocalDateTime now = LocalDateTime.now();
            ps.setString(1, sagaId);
            ps.setString(2, sagaName);
            ps.setString(3, SagaStatus.RUNNING.name());
            ps.setTimestamp(4, Timestamp.valueOf(now));
            ps.setTimestamp(5, Timestamp.valueOf(now));
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new StoreException("createSaga " + sagaId, e);
        }
    }

    @Override
    public synchronized void updateSagaStatus(String sagaId, SagaStatus status, String failureReason) {
        String sql = "UPDATE saga_instance SET status = ?, failure_reason = ?, updated_at = ? WHERE saga_id = ?";
        try (Connection c = connection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, status.name());
            ps.setString(2, failureReason);
            ps.setTimestamp(3, Timestamp.valueOf(LocalDateTime.now()));
            ps.setString(4, sagaId);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new StoreException("updateSagaStatus " + sagaId, e);
        }
    }

    @Override
    public synchronized Optional<SagaRecord> loadSaga(String sagaId) {
        String sql = "SELECT saga_id, saga_name, status, failure_reason FROM saga_instance WHERE saga_id = ?";
        try (Connection c = connection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, sagaId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(toSagaRecord(rs));
                }
                return Optional.empty();
            }
        } catch (SQLException e) {
            throw new StoreException("loadSaga " + sagaId, e);
        }
    }

    @Override
    public synchronized List<SagaRecord> loadIncomplete() {
        String sql = "SELECT saga_id, saga_name, status, failure_reason FROM saga_instance"
                + " WHERE status IN ('RUNNING', 'COMPENSATING') ORDER BY created_at";
        try (Connection c = connection();
             PreparedStatement ps = c.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            List<SagaRecord> out = new ArrayList<>();
            while (rs.next()) {
                out.add(toSagaRecord(rs));
            }
            return out;
        } catch (SQLException e) {
            throw new StoreException("loadIncomplete", e);
        }
    }

    @Override
    public synchronized void insertStep(String sagaId, int index, String name, String groupId) {
        String sql = "INSERT INTO saga_step (saga_id, step_idx, step_name, group_id, status, attempts, updated_at)"
                + " VALUES (?, ?, ?, ?, ?, 0, ?)";
        try (Connection c = connection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, sagaId);
            ps.setInt(2, index);
            ps.setString(3, name);
            ps.setString(4, groupId);
            ps.setString(5, StepStatus.PENDING.name());
            ps.setTimestamp(6, Timestamp.valueOf(LocalDateTime.now()));
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new StoreException("insertStep " + sagaId + "#" + index, e);
        }
    }

    @Override
    public synchronized boolean casStepStatus(String sagaId, int index, StepStatus expected, StepStatus target) {
        String sql = "UPDATE saga_step SET status = ?, updated_at = ? WHERE saga_id = ? AND step_idx = ? AND status = ?";
        try (Connection c = connection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, target.name());
            ps.setTimestamp(2, Timestamp.valueOf(LocalDateTime.now()));
            ps.setString(3, sagaId);
            ps.setInt(4, index);
            ps.setString(5, expected.name());
            return ps.executeUpdate() == 1;
        } catch (SQLException e) {
            throw new StoreException("casStepStatus " + sagaId + "#" + index, e);
        }
    }

    @Override
    public synchronized void forceStepStatus(String sagaId, int index, StepStatus target) {
        String sql = "UPDATE saga_step SET status = ?, updated_at = ? WHERE saga_id = ? AND step_idx = ?";
        try (Connection c = connection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, target.name());
            ps.setTimestamp(2, Timestamp.valueOf(LocalDateTime.now()));
            ps.setString(3, sagaId);
            ps.setInt(4, index);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new StoreException("forceStepStatus " + sagaId + "#" + index, e);
        }
    }

    @Override
    public synchronized void setStepAttempts(String sagaId, int index, int attempts) {
        String sql = "UPDATE saga_step SET attempts = ?, updated_at = ? WHERE saga_id = ? AND step_idx = ?";
        try (Connection c = connection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setInt(1, attempts);
            ps.setTimestamp(2, Timestamp.valueOf(LocalDateTime.now()));
            ps.setString(3, sagaId);
            ps.setInt(4, index);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new StoreException("setStepAttempts " + sagaId + "#" + index, e);
        }
    }

    @Override
    public synchronized List<StepRecord> loadSteps(String sagaId) {
        String sql = "SELECT step_idx, step_name, group_id, status, attempts FROM saga_step"
                + " WHERE saga_id = ? ORDER BY step_idx";
        try (Connection c = connection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, sagaId);
            try (ResultSet rs = ps.executeQuery()) {
                List<StepRecord> out = new ArrayList<>();
                while (rs.next()) {
                    String group = rs.getString(3);
                    out.add(new StepRecord(
                            rs.getInt(1),
                            rs.getString(2),
                            rs.wasNull() ? null : group,
                            StepStatus.valueOf(rs.getString(4)),
                            rs.getInt(5)));
                }
                return out;
            }
        } catch (SQLException e) {
            throw new StoreException("loadSteps " + sagaId, e);
        }
    }

    @Override
    public synchronized void appendEvent(String sagaId, EventType type, String detail) {
        String sql = "INSERT INTO saga_event (saga_id, event_type, detail, created_at) VALUES (?, ?, ?, ?)";
        try (Connection c = connection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, sagaId);
            ps.setString(2, type.name());
            ps.setString(3, detail);
            ps.setTimestamp(4, Timestamp.valueOf(LocalDateTime.now()));
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new StoreException("appendEvent " + sagaId, e);
        }
    }

    @Override
    public synchronized List<SagaEvent> loadEvents(String sagaId) {
        String sql = "SELECT id, saga_id, event_type, detail, created_at FROM saga_event"
                + " WHERE saga_id = ? ORDER BY id";
        try (Connection c = connection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, sagaId);
            try (ResultSet rs = ps.executeQuery()) {
                List<SagaEvent> out = new ArrayList<>();
                while (rs.next()) {
                    out.add(new SagaEvent(
                            rs.getLong(1),
                            rs.getString(2),
                            EventType.valueOf(rs.getString(3)),
                            rs.getString(4),
                            rs.getTimestamp(5).toLocalDateTime()));
                }
                return out;
            }
        } catch (SQLException e) {
            throw new StoreException("loadEvents " + sagaId, e);
        }
    }

    private SagaRecord toSagaRecord(ResultSet rs) throws SQLException {
        return new SagaRecord(
                rs.getString(1),
                rs.getString(2),
                SagaStatus.valueOf(rs.getString(3)),
                rs.getString(4));
    }

    /** Unchecked wrapper so the engine does not drown in SQLException. */
    public static class StoreException extends RuntimeException {
        public StoreException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
