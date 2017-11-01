package cn.edu.ruc.iir.rainbow.layout.seekcost;

public class LinearSeekCostFunction implements SeekCostFunction
{
    private static final double K = 0.005;

    @Override
    public double calculate(double distance)
    {
        return distance * K;
    }

}
