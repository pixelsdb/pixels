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
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.daemon.metrics;

import io.pixelsdb.pixels.common.metrics.NamedCost;
import io.pixelsdb.pixels.common.metrics.ReadPerfMetrics;
import com.alibaba.fastjson.JSON;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class TestJsonParse
{
    @Test
    public void test () throws IOException
    {
        StringBuilder json = new StringBuilder();
        BufferedReader reader = new BufferedReader(new InputStreamReader(this.getClass()
                .getResourceAsStream("/reader-perf-metrics.json")));
        String line;
        while ((line = reader.readLine()) != null)
        {
            json.append(line);
        }
        String jsonString = json.toString();
        long start = System.currentTimeMillis();
        ReadPerfMetrics metrics = JSON.parseObject(jsonString, ReadPerfMetrics.class);
        NamedCost constCost = new NamedCost();
        constCost.setName(JSON.toJSONString(metrics));
        constCost.setMs(1.2);
        metrics.addLambda(constCost);
        jsonString = JSON.toJSONString(metrics);
        ReadPerfMetrics metrics1 = JSON.parseObject(jsonString, ReadPerfMetrics.class);
        System.out.println(metrics1.getLambda().get(3).getName());
        System.out.println(System.currentTimeMillis() - start);
        System.out.println(jsonString);
        System.out.println(metrics.getSeek().get(1).getMs());
        System.out.println(metrics.getSeqRead().get(0).getBytes());
        System.out.println(metrics.getLambda().get(2).getName());
    }
}
