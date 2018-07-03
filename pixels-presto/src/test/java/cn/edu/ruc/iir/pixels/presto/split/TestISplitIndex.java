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
        List<AccessPattern> accessPatterns = new ArrayList<>();
        int i = 0;
        while ((line = workloadReader.readLine()) != null)
        {
            String[] columns = line.split("\t")[2].split(",");
            AccessPattern accessPattern = new AccessPattern();
            for (String column : columns)
            {
                accessPattern.addColumn(column);
            }
            accessPattern.setSplitSize(i++);
            accessPatterns.add(accessPattern);
        }
        workloadReader.close();
        Index index = new Inverted(columnOrder, accessPatterns);
        IndexFactory.Instance().cacheIndex(entry, index);
        index = IndexFactory.Instance().getIndex(new IndexEntry("test", "t1"));
        ColumnSet columnSet = new ColumnSet();
        String[] columns = {"QueryDate_","Market","IsBotVNext","IsNormalQuery","Vertical","AppInfoServerName","AppInfoClientName","QueryDate_","TrafficCount"};
        for (String column : columns)
        {
            columnSet.addColumn(column);
        }
        System.out.println(index.search(columnSet).toString());

    }
}
