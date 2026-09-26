package com.example.saga.core;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Restart-recovery tests: the engine "crashes" mid-saga (simulated by
 * {@link SagaCrashException}), a brand-new engine is started against the same
 * persistent H2 database file, and {@link SagaEngine#recover()} must drive the
 * interrupted saga to its terminal state without re-executing finished work.
 */
class RecoveryTest {

    @TempDir
    Path dbDir;

    private JdbcDataSource fileDataSource(String name) {
        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:file:" + dbDir.resolve(name).toAbsolutePath() + ";DB_CLOSE_DELAY=-1");
        ds.setUser("sa");
        return ds;
    }

    @Test
    void runningSagaIsResumedAfterRestart() {
        AtomicInteger step1Runs = new AtomicInteger();
        AtomicInteger step2Runs = new AtomicInteger();
        AtomicInteger step3Runs = new AtomicInteger();

        SagaDefinition saga = SagaBuilder.named("recoverable")
                .step("s1", ctx -> step1Runs.incrementAndGet(), ctx -> { })
                .step("s2", ctx -> step2Runs.incrementAndGet(), ctx -> { })
                .step("s3", ctx -> step3Runs.incrementAndGet(), ctx -> { })
                .build();

        // First "process": crashes right after step 1.
        SagaEngine firstEngine = new SagaEngine(fileDataSource("recovery1"),
                FaultInjection.crashAfterStep(1));
        firstEngine.register(saga);
        assertThrows(SagaCrashException.class, () -> firstEngine.execute(saga));
        assertEquals(1, step1Runs.get());
        assertEquals(0, step2Runs.get());

        // Second "process": same database file, no faults. Recovers and finishes.
        SagaEngine secondEngine = new SagaEngine(fileDataSource("recovery1"));
        secondEngine.register(saga);
        List<SagaResult> recovered = secondEngine.recover();

        assertEquals(1, recovered.size());
        SagaResult result = recovered.get(0);
        assertEquals(SagaStatus.COMPLETED, result.status());
        assertEquals(1, step1Runs.get(), "step 1 must not be re-executed after recovery");
        assertEquals(1, step2Runs.get());
        assertEquals(1, step3Runs.get());
        assertTrue(result.timeline().contains("SAGA_RECOVERED"));
    }

    @Test
    void compensatingSagaIsResumedAfterRestartAndCompensationStaysIdempotent() {
        AtomicInteger compensationRuns = new AtomicInteger();

        SagaDefinition saga = SagaBuilder.named("recoverable-compensation")
                .step("s1", ctx -> { }, ctx -> compensationRuns.incrementAndGet())
                .step(SagaStep.of("s2", ctx -> {
                    throw new IllegalStateException("always fails");
                }, ctx -> { }).withRetry(CoreTestSupport.fastRetry(1)))
                .build();

        // First "process": step 2 fails, compensation of step 1 runs, then crash.
        SagaEngine firstEngine = new SagaEngine(fileDataSource("recovery2"),
                FaultInjection.none().andCrashAfterCompensationOfStep(1));
        firstEngine.register(saga);
        assertThrows(SagaCrashException.class, () -> firstEngine.execute(saga));
        assertEquals(1, compensationRuns.get());

        // Second "process": resumes the COMPENSATING saga. The barrier must make the
        // repeated compensation of step 1 a no-op.
        SagaEngine secondEngine = new SagaEngine(fileDataSource("recovery2"));
        secondEngine.register(saga);
        List<SagaResult> recovered = secondEngine.recover();

        assertEquals(1, recovered.size());
        assertEquals(SagaStatus.COMPENSATED, recovered.get(0).status());
        assertEquals(1, compensationRuns.get(),
                "compensation executed before the crash must not run again after recovery");
    }
}
