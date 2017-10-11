package cn.edu.ruc.iir.rainbow.layout.seekcost;

import cn.edu.ruc.iir.rainbow.layout.domian.Coordinate;
import cn.edu.ruc.iir.rainbow.layout.domian.Line;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class SimulatedSeekCostFunction implements SeekCostFunction
{
    private List<Line> segments = null;
    private long interval = 0;
    private double K = 0;

    public SimulatedSeekCostFunction(long interval, List<Coordinate> points)
    {
        this.interval = interval;
        segments = new ArrayList<Line>();
        Collections.sort(points);

        for (int i = 0; i < points.size() - 1; ++i)
        {
            Coordinate point = points.get(i);
            double x = point.getX();
            double y = point.getY();
            Coordinate point1 = points.get(i + 1);
            double x1 = point1.getX();
            double y1 = point1.getY();
            double slope = (y1 - y) / (x1 - x);
            Line line = new Line(point, slope);
            segments.add(line);
        }

        Coordinate lastPoint = points.get(points.size() - 1);
        this.K = lastPoint.getY() / Math.sqrt(lastPoint.getX());
    }

    @Override
    public double calculate(double distance)
    {
        int id = (int) (distance / this.interval);
        if (id < this.segments.size())
        {
            return this.segments.get(id).getY(distance);
        } else
        {
            return this.K * Math.sqrt(distance);
        }
    }

}
