package cn.edu.ruc.iir.pixels.common.metrics;

import cn.edu.ruc.iir.pixels.common.utils.ConfigFactory;
import io.prometheus.client.Collector;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.common.TextFormat;

import java.io.IOException;
import java.io.StringWriter;
import java.util.*;

/**
 * This class is not thread safe.
 * It is only to be used by metrics server, which is a single thread.
 */
public class ReadPerfHistogram
{
    private static final int Interval;

    static
    {
        Interval = Integer.parseInt(ConfigFactory.Instance().getProperty("metric.bytesms.interval"));
    }

    private Map<Long, List<Double>> seqReadCosts = new HashMap<>();

    private Map<Long, List<Double>> seekCosts = new HashMap<>();

    private List<NamedCost> lambdaCosts = new ArrayList<>();

    private static void addCost (Map<Long, List<Double>> costs, BytesMsCost cost)
    {
        long length = cost.getBytes() / Interval * Interval;
        if (cost.getBytes() % Interval > Interval / 2)
        {
            length += Interval;
        }
        if (costs.containsKey(length))
        {
            costs.get(length).add(cost.getMs());
        }
        else
        {
            List<Double> costList = new ArrayList<>();
            costList.add(cost.getMs());
            costs.put(length, costList);
        }
    }

    public void addSeqReadCost (BytesMsCost cost)
    {
        addCost(this.seqReadCosts, cost);
    }

    public void addSeekCost (BytesMsCost cost)
    {
        addCost(this.seekCosts, cost);
    }

    public void addLambdaCost (NamedCost cost)
    {
        this.lambdaCosts.add(cost);
    }

    public void addMetrics (ReadPerfMetrics metrics)
    {
        for (BytesMsCost cost : metrics.getSeqRead())
        {
            this.addSeqReadCost(cost);
        }
        for (BytesMsCost cost : metrics.getSeek())
        {
            this.addSeekCost(cost);
        }
        for (NamedCost cost : metrics.getLambda())
        {
            this.addLambdaCost(cost);
        }
    }

    public String toPromTextFormat () throws IOException
    {
        Gauge seqRead = Gauge.build().help("pixels sequential read cost").name("pixels_seq_read_cost")
                .labelNames("length").create();
        for (Map.Entry<Long, List<Double>> entry : this.seqReadCosts.entrySet())
        {
            double sum  = 0;
            for (double cost : entry.getValue())
            {
                sum += cost;
            }
            seqRead.labels(entry.getKey().toString()).inc(sum / entry.getValue().size());
        }

        Gauge seek = Gauge.build().help("pixels seek cost").name("pixels_seek_cost")
                .labelNames("distance").create();
        for (Map.Entry<Long, List<Double>> entry : this.seekCosts.entrySet())
        {
            double sum  = 0;
            for (double cost : entry.getValue())
            {
                sum += cost;
            }
            seek.labels(entry.getKey().toString()).inc(sum / entry.getValue().size());
        }

        Gauge lambda = Gauge.build().help("pixels read task lambda cost").name("pixels_read_lambda_cost")
                .labelNames("name").create();
        for (NamedCost cost : this.lambdaCosts)
        {
            lambda.labels(cost.getName()).inc(cost.getMs());
        }

        Gauge interval = Gauge.build().help("the interval between two continuous bytesms metrics")
                .name("pixels_bytesms_interval").create();
        interval.inc(Interval);

        Vector<Collector.MetricFamilySamples> seqReadVector = new Vector<>(seqRead.collect());
        Vector<Collector.MetricFamilySamples> seekVector = new Vector<>(seek.collect());
        Vector<Collector.MetricFamilySamples> lambdaVector = new Vector<>(lambda.collect());
        Vector<Collector.MetricFamilySamples> intervalVector = new Vector<>(interval.collect());

        StringWriter writer = new StringWriter();
        TextFormat.write004(writer, intervalVector.elements());
        TextFormat.write004(writer, seqReadVector.elements());
        TextFormat.write004(writer, seekVector.elements());
        TextFormat.write004(writer, lambdaVector.elements());
        String str = writer.toString();
        writer.close();
        return str;
    }
}
