package io.saga.coordinator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.sql.DataSource;

/**
 * JDBC persistence for saga instances, per-step results and the event
 * timeline. Backed by embedded H2; every state transition is flushed so a
 * crashed process can resume from the last persisted state.
 */
public final class SagaRepository {

    private final DataSource dataSource;

    public SagaRepository(DataSource dataSource) {
        this.dataSource = dataSource;
        initSchema();
    }

    private void initSchema() {
        try (Connection c = dataSource.getConnection(); Statement s = c.createStatement()) {
            s.execute("""
                    CREATE TABLE IF NOT EXISTS saga_instance (
                      saga_id     VARCHAR(64) PRIMARY KEY,
                      name        VARCHAR(255) NOT NULL,
                      status      VARCHAR(32) NOT NULL,
                      fail_reason VARCHAR(1024),
                      created_at  TIMESTAMP WITH TIME ZONE NOT NULL,
                      updated_at  TIMESTAMP WITH TIME ZONE NOT NULL
                    )""");
            s.execute("""
                    CREATE TABLE IF NOT EXISTS saga_step (
                      saga_id    VARCHAR(64) NOT NULL,
                      step_name  VARCHAR(255) NOT NULL,
                      seq        INT NOT NULL,
                      group_name VARCHAR(255),
                      status     VARCHAR(32) NOT NULL,
                      attempts   INT NOT NULL DEFAULT 0,
                      error      VARCHAR(1024),
                      updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
                      PRIMARY KEY (saga_id, step_name)
                    )""");
            s.execute("""
                    CREATE TABLE IF NOT EXISTS saga_event (
                      id         BIGINT AUTO_INCREMENT PRIMARY KEY,
                      saga_id    VARCHAR(64) NOT NULL,
                      ts         TIMESTAMP WITH TIME ZONE NOT NULL,
                      type       VARCHAR(64) NOT NULL,
                      message    VARCHAR(1024)
                    )""");
        } catch (SQLException e) {
            throw new IllegalStateException("failed to initialise saga schema", e);
        }
    }

    public void insertSaga(String sagaId, String name) {
        String sql = "INSERT INTO saga_instance (saga_id, name, status, created_at, updated_at) VALUES (?,?,?,?,?)";
        try (Connection c = dataSource.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            Timestamp now = Timestamp.from(Instant.now());
            ps.setString(1, sagaId);
            ps.setString(2, name);
            ps.setString(3, SagaStatus.RUNNING.name());
            ps.setTimestamp(4, now);
            ps.setTimestamp(5, now);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new SagaPersistenceException(e);
        }
    }

    public void updateSagaStatus(String sagaId, SagaStatus status, String failReason) {
        String sql = "UPDATE saga_instance SET status=?, fail_reason=?, updated_at=? WHERE saga_id=?";
        try (Connection c = dataSource.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, status.name());
            ps.setString(2, truncate(failReason));
            ps.setTimestamp(3, Timestamp.from(Instant.now()));
            ps.setString(4, sagaId);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new SagaPersistenceException(e);
        }
    }

    public void insertStep(String sagaId, String stepName, int seq, String groupName) {
        String sql = "INSERT INTO saga_step (saga_id, step_name, seq, group_name, status, attempts, updated_at)"
                + " VALUES (?,?,?,?,?,?,?)";
        try (Connection c = dataSource.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, sagaId);
            ps.setString(2, stepName);
            ps.setInt(3, seq);
            ps.setString(4, groupName);
            ps.setString(5, StepStatus.PENDING.name());
            ps.setInt(6, 0);
            ps.setTimestamp(7, Timestamp.from(Instant.now()));
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new SagaPersistenceException(e);
        }
    }

    public void updateStep(String sagaId, String stepName, StepStatus status, int attempts, String error) {
        String sql = "UPDATE saga_step SET status=?, attempts=?, error=?, updated_at=? WHERE saga_id=? AND step_name=?";
        try (Connection c = dataSource.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, status.name());
            ps.setInt(2, attempts);
            ps.setString(3, truncate(error));
            ps.setTimestamp(4, Timestamp.from(Instant.now()));
            ps.setString(5, sagaId);
            ps.setString(6, stepName);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new SagaPersistenceException(e);
        }
    }

    public Optional<SagaInstance> findSaga(String sagaId) {
        String sql = "SELECT * FROM saga_instance WHERE saga_id=?";
        try (Connection c = dataSource.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, sagaId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return Optional.empty();
                }
                return Optional.of(new SagaInstance(
                        rs.getString("saga_id"), rs.getString("name"),
                        SagaStatus.valueOf(rs.getString("status")), rs.getString("fail_reason"),
                        rs.getTimestamp("created_at").toInstant(), rs.getTimestamp("updated_at").toInstant()));
            }
        } catch (SQLException e) {
            throw new SagaPersistenceException(e);
        }
    }

    /** Sagas that were in flight when the process stopped and must be resumed. */
    public List<SagaInstance> findIncomplete() {
        String sql = "SELECT * FROM saga_instance WHERE status IN ('RUNNING','COMPENSATING') ORDER BY created_at";
        List<SagaInstance> result = new ArrayList<>();
        try (Connection c = dataSource.getConnection();
                PreparedStatement ps = c.prepareStatement(sql);
                ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                result.add(new SagaInstance(
                        rs.getString("saga_id"), rs.getString("name"),
                        SagaStatus.valueOf(rs.getString("status")), rs.getString("fail_reason"),
                        rs.getTimestamp("created_at").toInstant(), rs.getTimestamp("updated_at").toInstant()));
            }
            return result;
        } catch (SQLException e) {
            throw new SagaPersistenceException(e);
        }
    }

    public List<StepRecord> loadSteps(String sagaId) {
        String sql = "SELECT * FROM saga_step WHERE saga_id=? ORDER BY seq";
        List<StepRecord> result = new ArrayList<>();
        try (Connection c = dataSource.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, sagaId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    result.add(new StepRecord(
                            rs.getString("saga_id"), rs.getString("step_name"), rs.getInt("seq"),
                            rs.getString("group_name"), StepStatus.valueOf(rs.getString("status")),
                            rs.getInt("attempts"), rs.getString("error"),
                            rs.getTimestamp("updated_at").toInstant()));
                }
            }
            return result;
        } catch (SQLException e) {
            throw new SagaPersistenceException(e);
        }
    }

    public Optional<StepRecord> findStep(String sagaId, String stepName) {
        return loadSteps(sagaId).stream().filter(s -> s.stepName().equals(stepName)).findFirst();
    }

    public void addEvent(String sagaId, String type, String message) {
        String sql = "INSERT INTO saga_event (saga_id, ts, type, message) VALUES (?,?,?,?)";
        try (Connection c = dataSource.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, sagaId);
            ps.setTimestamp(2, Timestamp.from(Instant.now()));
            ps.setString(3, type);
            ps.setString(4, truncate(message));
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new SagaPersistenceException(e);
        }
    }

    public List<SagaEvent> loadEvents(String sagaId) {
        String sql = "SELECT * FROM saga_event WHERE saga_id=? ORDER BY id";
        List<SagaEvent> result = new ArrayList<>();
        try (Connection c = dataSource.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, sagaId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    result.add(new SagaEvent(rs.getLong("id"), rs.getString("saga_id"),
                            rs.getTimestamp("ts").toInstant(), rs.getString("type"), rs.getString("message")));
                }
            }
            return result;
        } catch (SQLException e) {
            throw new SagaPersistenceException(e);
        }
    }

    private static String truncate(String s) {
        if (s == null || s.length() <= 1024) {
            return s;
        }
        return s.substring(0, 1021) + "...";
    }

    public static final class SagaPersistenceException extends RuntimeException {
        SagaPersistenceException(SQLException cause) {
            super(cause);
        }
    }
}
