package com.example.saga.demo;

import com.example.saga.core.FaultInjection;
import com.example.saga.core.FaultInjector;
import com.example.saga.core.SagaResult;
import com.example.saga.core.SagaStatus;
import com.example.saga.participant.BarrierResult;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** End-to-end demo scenarios driven by the fault-injection framework. */
class SagaDemoTest {

    @Test
    void happyPathCompletes() {
        SagaResult result = SagaDemoMain.scenario("demo: happy path", FaultInjector.NONE);
        assertEquals(SagaStatus.COMPLETED, result.status());
    }

    @Test
    void faultBeforeStep2CompensatesStep1() {
        SagaResult result = SagaDemoMain.scenario("demo: fault before step 2",
                FaultInjection.failBeforeStep(2));
        assertEquals(SagaStatus.COMPENSATED, result.status());
        assertTrue(result.failReason().contains("inventory.reserve"));
        assertTrue(result.timeline().contains("COMPENSATION_SUCCEEDED"));
    }

    @Test
    void faultAfterStep2CompensatesBothPriorWork() {
        SagaResult result = SagaDemoMain.scenario("demo: fault after step 2",
                FaultInjection.failAfterStep(2));
        assertEquals(SagaStatus.COMPENSATED, result.status());
    }

    @Test
    void nullRollbackIsAbsorbedByBarrier() throws Exception {
        assertEquals(BarrierResult.NULL_ROLLBACK, SagaDemoMain.nullRollbackScenario());
    }

    @Test
    void suspensionRejectsLateTry() throws Exception {
        assertEquals(BarrierResult.SUSPENDED, SagaDemoMain.suspensionScenario());
    }
}
