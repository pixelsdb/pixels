package cn.edu.ruc.iir.rainbow.layout.algorithm;

public class AlgorithmExecutor implements Runnable
{
    private Algorithm algorithm = null;

    protected AlgorithmExecutor(Algorithm algo)
    {
        this.algorithm = algo;
    }

    @Override
    public void run()
    {
        this.algorithm.runAlgorithm();
    }
}
