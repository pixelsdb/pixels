package cn.edu.ruc.iir.pixels.daemon.metric;

import io.prometheus.client.Collector;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.common.TextFormat;
import org.junit.Test;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Enumeration;
import java.util.Vector;

public class TestPrometheus
{
    @Test
    public void testGauge () throws IOException
    {
        Gauge gauge = Gauge.build().help("pixels seek cost").name("pixels_seek_cost").labelNames("distance").create();
        gauge.labels("1024").inc(20.4);
        gauge.labels("2048").inc(30.5);
        StringWriter writer = new StringWriter();
        Vector<Collector.MetricFamilySamples> samplesVector = new Vector<>(gauge.collect());
        Enumeration<Collector.MetricFamilySamples> samplesEnumeration = samplesVector.elements();
        TextFormat.write004(writer, samplesEnumeration);
        System.out.println(writer.toString());
    }
}
