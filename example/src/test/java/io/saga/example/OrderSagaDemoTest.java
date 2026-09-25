package io.saga.example;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.saga.coordinator.SagaStatus;
import io.saga.example.OrderSagaDemo.ScenarioResult;
import java.util.List;
import org.junit.jupiter.api.Test;

class OrderSagaDemoTest {

    @Test
    void allDemoScenariosReachTheirExpectedFinalState() {
        List<ScenarioResult> scenarios = OrderSagaDemo.runScenarios();
        assertEquals(6, scenarios.size());

        assertScenario(scenarios.get(0), SagaStatus.COMPLETED.name());
        assertScenario(scenarios.get(1), SagaStatus.COMPENSATED.name());
        assertScenario(scenarios.get(2), SagaStatus.COMPENSATED.name());
        assertScenario(scenarios.get(3), "ABSORBED");
        assertScenario(scenarios.get(4), "REJECTED-LATE-FORWARD");
        assertScenario(scenarios.get(5), SagaStatus.COMPENSATED.name());

        ScenarioResult retryScenario = scenarios.get(5);
        assertTrue(retryScenario.failReason().contains("attempts exhausted"), retryScenario.failReason());
        assertTrue(retryScenario.timeline().stream().anyMatch(line -> line.contains("STEP_RETRY")));
        assertTrue(retryScenario.timeline().stream().anyMatch(line -> line.contains("SAGA_COMPENSATED")));
    }

    private static void assertScenario(ScenarioResult scenario, String expectedStatus) {
        assertEquals(expectedStatus, scenario.finalStatus(),
                "scenario '" + scenario.name() + "' ended in unexpected state");
    }
}
