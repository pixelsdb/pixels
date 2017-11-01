package cn.edu.ruc.iir.rainbow.layout.algorithm;

import cn.edu.ruc.iir.rainbow.common.cmd.ProgressListener;
import cn.edu.ruc.iir.rainbow.common.exception.NotMultiThreadedException;
import cn.edu.ruc.iir.rainbow.common.util.LogFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ExecutorContainer
{
    private ExecutorService executor = null;
    private Algorithm algo = null;

    public ExecutorContainer(Algorithm algo, int threadCount) throws NotMultiThreadedException
    {
        this.executor = Executors.newCachedThreadPool();
        this.algo = algo;
        algo.setup();
        if (algo.isMultiThreaded())
        {
            for (int i = 0; i < threadCount; ++i)
            {
                this.executor.execute(new AlgorithmExecutor(algo));
            }
        }
        else
        {
            if (threadCount == 1)
            {
                this.executor.execute(new AlgorithmExecutor(algo));
            }
            else
            {
                throw new NotMultiThreadedException("Algorithm " + algo.getClass().getName() + " is not multi-threaded.");
            }
        }
    }

    /**
     * Set completed percentage of the progressListener after each interval.
     * @param interval in seconds
     * @param progressListener
     * @throws InterruptedException
     */
    public void waitForCompletion(long interval, ProgressListener progressListener) throws InterruptedException
    {
        this.executor.shutdown();
        long computationBudget = this.algo.getComputationBudget();
        if (computationBudget <= 0)
        {
            LogFactory.Instance().getLog().debug("Computation budget {" + computationBudget + "} <= 0, force set to 1.");
            computationBudget = 1;
        }

        if (interval <= 0)
        {
            interval = 1;
        }

        double executedTime = 0;

        while (this.executor.awaitTermination(interval, TimeUnit.SECONDS) == false)
        {
            executedTime += interval;
            if (executedTime > computationBudget)
            {
                LogFactory.Instance().getLog().debug("Computation budget {" + computationBudget +
                        "} is not enough. We are not giving up. Another {" + interval + "} is given.");
                executedTime = computationBudget;
            }
            progressListener.setPercentage(executedTime/computationBudget);
        }
        this.executor.shutdownNow();
        this.algo.cleanup();
    }
}
