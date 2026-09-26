package com.example.saga.participant;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;

/**
 * JDBC (H2) backed store for participant barrier records.
 * The unique primary key (saga_id, step_name, barrier_type) is what makes
 * barrier insertion atomic and therefore safe against duplicate delivery.
 */
public class BarrierRepository {

    private final DataSource dataSource;

    public BarrierRepository(DataSource dataSource) {
        this.dataSource = dataSource;
        createSchema();
    }

    private void createSchema() {
        String sql = """
                CREATE TABLE IF NOT EXISTS participant_barrier (
                  saga_id      VARCHAR(64)  NOT NULL,
                  step_name    VARCHAR(128) NOT NULL,
                  barrier_type VARCHAR(16)  NOT NULL,
                  created_at   TIMESTAMP    NOT NULL,
                  PRIMARY KEY (saga_id, step_name, barrier_type)
                )
                """;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("failed to create participant_barrier table", e);
        }
    }

    /**
     * Inserts a barrier record if absent.
     *
     * @return true if this call inserted the record, false if it already existed
     */
    public boolean insertIfAbsent(String sagaId, String stepName, BarrierType type) {
        String sql = "INSERT INTO participant_barrier (saga_id, step_name, barrier_type, created_at) VALUES (?,?,?,?)";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, sagaId);
            ps.setString(2, stepName);
            ps.setString(3, type.name());
            ps.setTimestamp(4, Timestamp.from(Instant.now()));
            ps.executeUpdate();
            return true;
        } catch (SQLException e) {
            // H2 unique-constraint violation has SQL state 23505
            if ("23505".equals(e.getSQLState())) {
                return false;
            }
            throw new IllegalStateException("failed to insert barrier", e);
        }
    }

    public boolean exists(String sagaId, String stepName, BarrierType type) {
        String sql = "SELECT 1 FROM participant_barrier WHERE saga_id=? AND step_name=? AND barrier_type=?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, sagaId);
            ps.setString(2, stepName);
            ps.setString(3, type.name());
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        } catch (SQLException e) {
            throw new IllegalStateException("failed to query barrier", e);
        }
    }

    public void delete(String sagaId, String stepName, BarrierType type) {
        String sql = "DELETE FROM participant_barrier WHERE saga_id=? AND step_name=? AND barrier_type=?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, sagaId);
            ps.setString(2, stepName);
            ps.setString(3, type.name());
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("failed to delete barrier", e);
        }
    }
}
