/*
 * Copyright 2018 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Foobar.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.daemon.metric;

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
