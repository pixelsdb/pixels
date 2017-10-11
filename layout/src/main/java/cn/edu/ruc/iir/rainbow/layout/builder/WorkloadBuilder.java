package cn.edu.ruc.iir.rainbow.layout.builder;

import cn.edu.ruc.iir.rainbow.common.exception.ColumnNotFoundException;
import cn.edu.ruc.iir.rainbow.common.util.ConfigFactory;
import cn.edu.ruc.iir.rainbow.layout.domian.Column;
import cn.edu.ruc.iir.rainbow.layout.domian.Query;
import cn.edu.ruc.iir.rainbow.layout.domian.WorkloadPattern;

import java.io.*;
import java.util.*;

/**
 * Created by hank on 2015/4/28.
 */
public class WorkloadBuilder
{
    private WorkloadBuilder ()
    {

    }

    public static List<Query> build (File workloadFile, List<Column> columnOrder)
            throws IOException, ColumnNotFoundException
    {
        BufferedReader reader = new BufferedReader(new FileReader(workloadFile));

        Map<String, Column> columnMap = new HashMap<String, Column>();
        List<Query> workload = new ArrayList<Query>();

        for (Column column : columnOrder)
        {
            columnMap.put(column.getName().toLowerCase(), column);
        }

        String line;
        int qid = 0;
        while ((line = reader.readLine()) != null)
        {
            String[] tokens = line.split("\t");
            String[] columnNames = tokens[2].split(",");
            Query query = new Query(qid, tokens[0], Double.parseDouble(tokens[1]));
            for (String columnName : columnNames)
            {
                Column column = columnMap.get(columnName.toLowerCase());
                if (column == null)
                {
                    throw new ColumnNotFoundException("column " + columnName + " from query " +
                            query.getId() + " is not found in the schema.");
                }
                query.addColumnId(column.getId());
            }
            boolean noEquals = true;
            for (Query query1 : workload)
            {
                if (equalsColumnAccessSet(query.getColumnIds(), query1.getColumnIds()))
                {
                    query1.addWeight(query.getWeight());
                    noEquals = false;
                    break;
                }
            }
            if (noEquals)
            {
                qid++;
                workload.add(query);
            }
        }

        reader.close();

        return workload;
    }

    public static void saveAsWorkloadFile (File workloadFile, WorkloadPattern workloadPattern) throws IOException
    {
        BufferedWriter queryWriter = new BufferedWriter(new FileWriter(workloadFile));

        final String DUP_MARK = ConfigFactory.Instance().getProperty("dup.mark");

        for (Query query : workloadPattern.getQuerySet())
        {
            queryWriter.write(query.getSid() + "\t");
            queryWriter.write(query.getWeight() + "\t");
            Set<Column> pattern = workloadPattern.getColumnSet(query);
            boolean first = true;
            for (Column column : pattern)
            {
                if (first)
                {
                    queryWriter.write(column.isDuplicated() ?
                            column.getName() + DUP_MARK + column.getDupId() :column.getName());
                    first = false;
                }
                else
                {
                    queryWriter.write("," + (column.isDuplicated() ?
                            column.getName() + DUP_MARK + column.getDupId() :column.getName()));

                }
            }
            queryWriter.write("\n");
        }

        queryWriter.close();
    }

    private static boolean equalsColumnAccessSet (Set<Integer> cids1, Set<Integer> cids2)
    {
        if (cids1 == null || cids2 == null)
        {
            return false;
        }
        return cids2.containsAll(cids1) && cids1.size() == cids2.size();
    }
}
