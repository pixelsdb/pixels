package io.pixelsdb.pixels.common.metrics;

public class NamedCost
{
    private String name;
    private double ms;

    public NamedCost()
    {
    }

    public NamedCost(String name, double ms)
    {
        this.name = name;
        this.ms = ms;
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public double getMs()
    {
        return ms;
    }

    public void setMs(double ms)
    {
        this.ms = ms;
    }
}
