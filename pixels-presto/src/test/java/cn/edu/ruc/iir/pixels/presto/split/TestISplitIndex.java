package cn.edu.ruc.iir.pixels.presto.split;

import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TestISplitIndex
{
    @Test
    public void test () throws IOException
    {
        IndexEntry entry = new IndexEntry("test","t1");
        BufferedReader schemaReader = new BufferedReader(
                new FileReader(
                        "/home/hank/dev/idea-projects/pixels/pixels-presto/target/classes/105_schema.text"));
        List<String> columnOrder = new ArrayList<>();
        String line;
        while ((line = schemaReader.readLine()) != null)
        {
            columnOrder.add(line.split("\t")[0]);
        }
        schemaReader.close();
        BufferedReader workloadReader = new BufferedReader(
                new FileReader("/home/hank/dev/idea-projects/pixels/pixels-presto/target/classes/105_workload.text")
        );
        //Index index = new Inverted();
        //IndexFactory.Instance().cacheIndex(entry, null);
        System.out.println(this.getClass().getResource("/").getPath());
    }
}
