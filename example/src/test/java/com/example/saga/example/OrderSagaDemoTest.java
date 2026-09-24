package com.example.saga.example;

import com.example.saga.coordinator.SagaResult;
import com.example.saga.coordinator.SagaStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OrderSagaDemoTest {

    private final OrderSagaDemo demo = new OrderSagaDemo();

    @Test
    void happyPathCompletes() {
        assertEquals(SagaStatus.COMPLETED, demo.happyPath().status());
    }

    @Test
    void faultBeforeStepCompensates() {
        SagaResult result = demo.failBeforeStep();
        assertEquals(SagaStatus.COMPENSATED, result.status());
        assertTrue(demo.participantLog().stream().anyMatch(l -> l.contains("NULL ROLLBACK")));
    }

    @Test
    void faultAfterStepCompensates() {
        SagaResult result = demo.failAfterStep();
        assertEquals(SagaStatus.COMPENSATED, result.status());
        assertTrue(demo.participantLog().stream().anyMatch(l -> l.contains("refund 100")));
    }

    @Test
    void lostRequestLeadsToNullRollbackAndSuspension() throws Exception {
        SagaResult result = demo.nullRollbackAndSuspension();
        assertEquals(SagaStatus.COMPENSATED, result.status());
        assertTrue(demo.participantLog().stream().anyMatch(l -> l.contains("NULL ROLLBACK")));
    }

    @Test
    void crashedSagaIsRecovered(@TempDir Path dir) {
        SagaResult result = demo.crashAndRecover(dir.resolve("recovery-db"));
        assertEquals(SagaStatus.COMPLETED, result.status());
        assertTrue(result.timeline().stream().anyMatch(e -> e.type().equals("SAGA_RECOVERY")));
    }
}
