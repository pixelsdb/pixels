package cn.edu.ruc.iir.rainbow.layout.domian;

public class Line implements Comparable<Line>
{
    private Coordinate start;
    private double slope;

    public Line(Coordinate start, double slope)
    {
        this.start = start;
        this.slope = slope;
    }

    public double getY(double X)
    {
        return this.slope * (X - this.start.getX()) + this.start.getY();
    }

    @Override
    public int compareTo(Line line)
    {
        return this.start.compareTo(line.start);
    }
}
