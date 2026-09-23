package io.saga.core.fault;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Programmable fault injector for tests and demos. Supported injections:
 * <ul>
 *   <li>{@code failBeforeStep(n)}  - throw right before step n's forward action</li>
 *   <li>{@code failAfterStep(n)}   - throw right after step n's forward action succeeded</li>
 *   <li>{@code dropForwardOfStep(n)} - swallow step n's forward delivery (empty-rollback driver)</li>
 *   <li>{@code crashBeforeStep(n)} - raise a simulated process crash once (recovery driver)</li>
 * </ul>
 */
public class ConfigurableFaultInjector implements FaultInjector {

    private Integer failBeforeStep;
    private Integer failAfterStep;
    private Integer dropForwardOfStep;
    private Integer crashBeforeStep;
    private final Map<String, Boolean> firedOnce = new ConcurrentHashMap<>();

    public ConfigurableFaultInjector failBeforeStep(int stepIndex) {
        this.failBeforeStep = stepIndex;
        return this;
    }

    public ConfigurableFaultInjector failAfterStep(int stepIndex) {
        this.failAfterStep = stepIndex;
        return this;
    }

    public ConfigurableFaultInjector dropForwardOfStep(int stepIndex) {
        this.dropForwardOfStep = stepIndex;
        return this;
    }

    public ConfigurableFaultInjector crashBeforeStep(int stepIndex) {
        this.crashBeforeStep = stepIndex;
        return this;
    }

    @Override
    public Decision beforeStep(StepPoint point) {
        if (crashBeforeStep != null && crashBeforeStep == point.stepIndex()
                && firedOnce.putIfAbsent("crash:" + point.sagaId(), Boolean.TRUE) == null) {
            throw new SimulatedCrashException("simulated crash before step " + point.stepIndex()
                    + " (" + point.stepName() + ")");
        }
        if (failBeforeStep != null && failBeforeStep == point.stepIndex()) {
            throw new InjectedFaultException("injected fault before step " + point.stepIndex()
                    + " (" + point.stepName() + ")");
        }
        if (dropForwardOfStep != null && dropForwardOfStep == point.stepIndex()) {
            return Decision.SKIP_ACTION;
        }
        return Decision.PROCEED;
    }

    @Override
    public void afterStep(StepPoint point) {
        if (failAfterStep != null && failAfterStep == point.stepIndex()) {
            throw new InjectedFaultException("injected fault after step " + point.stepIndex()
                    + " (" + point.stepName() + ")");
        }
    }
}
