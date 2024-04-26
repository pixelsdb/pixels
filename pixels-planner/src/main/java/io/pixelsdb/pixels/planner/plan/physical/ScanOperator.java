package io.pixelsdb.pixels.planner.plan.physical;

import com.google.common.collect.ImmutableList;
import io.pixelsdb.pixels.common.turbo.Output;
import io.pixelsdb.pixels.planner.coordinate.PlanCoordinator;
import io.pixelsdb.pixels.planner.plan.physical.input.ScanInput;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * @author hank
 * @create 2024-04-26
 */
public abstract class ScanOperator extends Operator
{
    /**
     * The scan inputs of the scan workers that produce the partial aggregation
     * results. It should be empty if child is not null.
     */
    protected final List<ScanInput> scanInputs;
    /**
     * The outputs of the scan workers.
     */
    protected CompletableFuture<? extends Output>[] scanOutputs = null;

    public ScanOperator(String name, List<ScanInput> scanInputs)
    {
        super(name);
        requireNonNull(scanInputs, "scanInputs is null");
        checkArgument(!scanInputs.isEmpty(), "scanInputs is empty");
        this.scanInputs = ImmutableList.copyOf(scanInputs);
        for (ScanInput scanInput : this.scanInputs)
        {
            scanInput.setOperatorName(name);
        }
    }

    public List<ScanInput> getScanInputs()
    {
        return scanInputs;
    }

    @Override
    public void initPlanCoordinator(PlanCoordinator planCoordinator, int parentStageId, boolean wideDependOnParent)
    {

    }
}
