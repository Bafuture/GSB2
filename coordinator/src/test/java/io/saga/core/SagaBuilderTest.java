package io.saga.core;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SagaBuilderTest {

    private static final SagaAction NOOP = ctx -> { };

    @Test
    void rejectsDuplicateStepNames() {
        SagaBuilderDslProbe builder = new SagaBuilderDslProbe();
        assertThrows(IllegalArgumentException.class, () -> builder.duplicate());
    }

    @Test
    void rejectsSecondParallelGroup() {
        assertThrows(IllegalStateException.class, () -> Saga.define("x")
                .parallel("g1", p -> p.step("a", NOOP, NOOP).step("b", NOOP, NOOP))
                .parallel("g2", p -> p.step("c", NOOP, NOOP).step("d", NOOP, NOOP)));
    }

    @Test
    void rejectsEmptySagaAndTinyGroups() {
        assertThrows(IllegalStateException.class, () -> Saga.define("x").build());
        assertThrows(IllegalArgumentException.class, () -> Saga.define("x")
                .parallel("g", p -> p.step("a", NOOP, NOOP)));
    }

    @Test
    void assignsGlobalIndexesAcrossSerialAndGroupSteps() {
        SagaDefinition saga = Saga.define("x")
                .step("a", NOOP, NOOP)
                .parallel("g", p -> p.step("b", NOOP, NOOP).step("c", NOOP, NOOP))
                .step("d", NOOP, NOOP)
                .build();
        assertEquals(0, saga.indexOf("a"));
        assertEquals(1, saga.indexOf("b"));
        assertEquals(2, saga.indexOf("c"));
        assertEquals(3, saga.indexOf("d"));
        assertEquals(4, saga.getFlatSteps().size());
    }

    /** Helper so the duplicate-name check reads naturally. */
    private static final class SagaBuilderDslProbe {
        void duplicate() {
            Saga.define("x").step("a", NOOP, NOOP).step("a", NOOP, NOOP);
        }
    }
}
