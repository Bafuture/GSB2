package io.saga.core;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * An ordered execution plan. Items are either single serial steps or one
 * parallel group whose steps run concurrently and complete as a unit.
 */
public final class SagaDefinition {

    /** One item of the execution plan: either a serial step or a parallel group. */
    public sealed interface PlanItem permits StepItem, GroupItem { }

    public record StepItem(StepDefinition step) implements PlanItem { }

    public record GroupItem(String groupId, List<StepDefinition> steps) implements PlanItem { }

    private final String name;
    private final List<PlanItem> plan;
    private final List<StepDefinition> flatSteps;

    public SagaDefinition(String name, List<PlanItem> plan) {
        this.name = name;
        this.plan = Collections.unmodifiableList(new ArrayList<>(plan));
        List<StepDefinition> flat = new ArrayList<>();
        for (PlanItem item : this.plan) {
            if (item instanceof StepItem s) {
                flat.add(s.step());
            } else if (item instanceof GroupItem g) {
                flat.addAll(g.steps());
            }
        }
        for (int i = 0; i < flat.size(); i++) {
            flat.get(i).assignIndex(i);
        }
        this.flatSteps = Collections.unmodifiableList(flat);
    }

    public String getName() { return name; }
    public List<PlanItem> getPlan() { return plan; }
    public List<StepDefinition> getFlatSteps() { return flatSteps; }

    public StepDefinition getStep(int index) { return flatSteps.get(index); }

    public int indexOf(String stepName) {
        for (StepDefinition s : flatSteps) {
            if (s.getName().equals(stepName)) {
                return s.getIndex();
            }
        }
        throw new IllegalArgumentException("no step named " + stepName + " in saga " + name);
    }
}
