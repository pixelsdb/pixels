package cn.edu.ruc.iir.rainbow.layout.seekcost;

public interface SeekCostFunction
{
    enum Type
    {
        LINEAR,
        POWER,
        SIMULATED
    }

    double calculate(double distance);
}
