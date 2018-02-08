package cn.edu.ruc.iir.pixels.daemon.metric;

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
        jsonString = JSON.toJSONString(metrics);
        System.out.println(System.currentTimeMillis() - start);
        System.out.println(jsonString);
        System.out.println(metrics.getSeek().get(1).getMs());
        System.out.println(metrics.getSeqRead().get(0).getBytes());
        System.out.println(metrics.getLambda().get(2).getName());
    }
}
