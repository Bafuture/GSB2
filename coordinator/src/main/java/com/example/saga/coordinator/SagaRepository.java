package com.example.saga.coordinator;

import org.h2.jdbcx.JdbcDataSource;

import javax.sql.DataSource;
import java.nio.file.Path;
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

/**
 * JDBC persistence for saga instances, step states and the event timeline.
 * Backed by embedded H2; use a file URL to survive process restarts.
 */
public class SagaRepository {

    private final DataSource dataSource;

    public SagaRepository(DataSource dataSource) {
        this.dataSource = dataSource;
        initSchema();
    }

    public static SagaRepository at(Path dbFile) {
        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:file:" + dbFile.toAbsolutePath());
        ds.setUser("sa");
        return new SagaRepository(ds);
    }

    public static SagaRepository inMemory(String name) {
        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:mem:" + name + ";DB_CLOSE_DELAY=-1");
        ds.setUser("sa");
        return new SagaRepository(ds);
    }

    private void initSchema() {
        try (Connection c = dataSource.getConnection(); Statement s = c.createStatement()) {
            s.execute("""
                    CREATE TABLE IF NOT EXISTS saga_instance (
                      saga_id        VARCHAR(64)  PRIMARY KEY,
                      saga_name      VARCHAR(128) NOT NULL,
                      status         VARCHAR(32)  NOT NULL,
                      failure_reason VARCHAR(2048),
                      created_at     TIMESTAMP    NOT NULL,
                      updated_at     TIMESTAMP    NOT NULL
                    )
                    """);
            s.execute("""
                    CREATE TABLE IF NOT EXISTS saga_step (
                      saga_id   VARCHAR(64)  NOT NULL,
                      step_seq  INT          NOT NULL,
                      step_name VARCHAR(128) NOT NULL,
                      status    VARCHAR(32)  NOT NULL,
                      attempts  INT          NOT NULL DEFAULT 0,
                      PRIMARY KEY (saga_id, step_seq)
                    )
                    """);
            s.execute("""
                    CREATE TABLE IF NOT EXISTS saga_event (
                      id          BIGINT AUTO_INCREMENT PRIMARY KEY,
                      saga_id     VARCHAR(64)   NOT NULL,
                      event_type  VARCHAR(64)   NOT NULL,
                      step_name   VARCHAR(128),
                      message     VARCHAR(2048),
                      occurred_at TIMESTAMP     NOT NULL
                    )
                    """);
        } catch (SQLException e) {
            throw new IllegalStateException("failed to initialize saga schema", e);
        }
    }

    public void createSaga(String sagaId, String sagaName) {
        Instant now = Instant.now();
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(
                     "INSERT INTO saga_instance (saga_id, saga_name, status, created_at, updated_at) VALUES (?,?,?,?,?)")) {
            ps.setString(1, sagaId);
            ps.setString(2, sagaName);
            ps.setString(3, SagaStatus.RUNNING.name());
            ps.setTimestamp(4, Timestamp.from(now));
            ps.setTimestamp(5, Timestamp.from(now));
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new SagaPersistenceException(e);
        }
    }

    public void createStep(String sagaId, int seq, String stepName) {
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(
                     "INSERT INTO saga_step (saga_id, step_seq, step_name, status, attempts) VALUES (?,?,?,?,0)")) {
            ps.setString(1, sagaId);
            ps.setInt(2, seq);
            ps.setString(3, stepName);
            ps.setString(4, StepStatus.PENDING.name());
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new SagaPersistenceException(e);
        }
    }

    public void updateSagaStatus(String sagaId, SagaStatus status, String failureReason) {
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(
                     "UPDATE saga_instance SET status=?, failure_reason=?, updated_at=? WHERE saga_id=?")) {
            ps.setString(1, status.name());
            ps.setString(2, failureReason);
            ps.setTimestamp(3, Timestamp.from(Instant.now()));
            ps.setString(4, sagaId);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new SagaPersistenceException(e);
        }
    }

    public void updateStepStatus(String sagaId, int seq, StepStatus status, int attempts) {
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(
                     "UPDATE saga_step SET status=?, attempts=? WHERE saga_id=? AND step_seq=?")) {
            ps.setString(1, status.name());
            ps.setInt(2, attempts);
            ps.setString(3, sagaId);
            ps.setInt(4, seq);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new SagaPersistenceException(e);
        }
    }

    public record SagaRecord(String sagaId, String sagaName, SagaStatus status, String failureReason) {
    }

    public Optional<SagaRecord> findSaga(String sagaId) {
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(
                     "SELECT saga_id, saga_name, status, failure_reason FROM saga_instance WHERE saga_id=?")) {
            ps.setString(1, sagaId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return Optional.empty();
                }
                String reason = rs.getString(4);
                return Optional.of(new SagaRecord(rs.getString(1), rs.getString(2),
                        SagaStatus.valueOf(rs.getString(3)), reason));
            }
        } catch (SQLException e) {
            throw new SagaPersistenceException(e);
        }
    }

    public record StepRecord(int seq, String stepName, StepStatus status, int attempts) {
    }

    public List<StepRecord> loadSteps(String sagaId) {
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(
                     "SELECT step_seq, step_name, status, attempts FROM saga_step WHERE saga_id=? ORDER BY step_seq")) {
            ps.setString(1, sagaId);
            List<StepRecord> steps = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    steps.add(new StepRecord(rs.getInt(1), rs.getString(2),
                            StepStatus.valueOf(rs.getString(3)), rs.getInt(4)));
                }
            }
            return steps;
        } catch (SQLException e) {
            throw new SagaPersistenceException(e);
        }
    }

    public List<SagaRecord> findIncompleteSagas() {
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(
                     "SELECT saga_id, saga_name, status, failure_reason FROM saga_instance " +
                             "WHERE status IN ('RUNNING','COMPENSATING') ORDER BY created_at")) {
            List<SagaRecord> sagas = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    sagas.add(new SagaRecord(rs.getString(1), rs.getString(2),
                            SagaStatus.valueOf(rs.getString(3)), rs.getString(4)));
                }
            }
            return sagas;
        } catch (SQLException e) {
            throw new SagaPersistenceException(e);
        }
    }

    public void recordEvent(String sagaId, String type, String stepName, String message) {
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(
                     "INSERT INTO saga_event (saga_id, event_type, step_name, message, occurred_at) VALUES (?,?,?,?,?)")) {
            ps.setString(1, sagaId);
            ps.setString(2, type);
            ps.setString(3, stepName);
            ps.setString(4, message);
            ps.setTimestamp(5, Timestamp.from(Instant.now()));
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new SagaPersistenceException(e);
        }
    }

    public List<SagaEvent> loadEvents(String sagaId) {
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(
                     "SELECT id, saga_id, event_type, step_name, message, occurred_at " +
                             "FROM saga_event WHERE saga_id=? ORDER BY id")) {
            ps.setString(1, sagaId);
            List<SagaEvent> events = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    events.add(new SagaEvent(rs.getLong(1), rs.getString(2), rs.getString(3),
                            rs.getString(4), rs.getString(5), rs.getTimestamp(6).toInstant()));
                }
            }
            return events;
        } catch (SQLException e) {
            throw new SagaPersistenceException(e);
        }
    }

    public static class SagaPersistenceException extends RuntimeException {
        public SagaPersistenceException(Throwable cause) {
            super(cause);
        }
    }
}
