package cn.edu.ruc.iir.rainbow.layout.domian;

public class Coordinate implements Comparable<Coordinate>
{
    private double y;
    private double x;

    public Coordinate(double x, double y)
    {
        this.x = x;
        this.y = y;
    }

    public double getX()
    {
        return x;
    }

    public double getY()
    {
        return y;
    }

    @Override
    public int compareTo(Coordinate coordinate)
    {
        if (this.x - coordinate.x < 0)
        {
            return -1;
        } else if (this.x - coordinate.x > 0)
        {
            return 1;
        } else
        {
            return 0;
        }
    }
}
