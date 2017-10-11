package cn.edu.ruc.iir.rainbow.layout.sql;

import cn.edu.ruc.iir.rainbow.common.util.ConfigFactory;
import cn.edu.ruc.iir.rainbow.common.util.InputFactory;
import cn.edu.ruc.iir.rainbow.common.util.OutputFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;

public class GenerateLoad
{
    private GenerateLoad () {}

    public static void Gen(boolean overWrite, String tableName, String schemaFilePath, String loadStatementPath) throws IOException
    {
        final String DUP_MARK = ConfigFactory.Instance().getProperty("dup.mark");

        try (BufferedReader reader = InputFactory.Instance().getReader(schemaFilePath);
             BufferedWriter writer = OutputFactory.Instance().getWriter(loadStatementPath))
        {
            String line, columnName, replicaName;
            writer.write("INSERT " + (overWrite ? "OVERWRITE" : "INTO") + " TABLE " + tableName + "\nSELECT\n");
            line = reader.readLine();

            replicaName = line.split("\t")[0];
            if (replicaName.contains(DUP_MARK))
            {
                columnName = replicaName.split(DUP_MARK)[0] + " AS " + replicaName;
            }
            else
            {
                columnName = replicaName;
            }
            writer.write(columnName);

            while ((line = reader.readLine()) != null)
            {
                replicaName = line.split("\t")[0];
                if (replicaName.contains(DUP_MARK))
                {
                    columnName = replicaName.split(DUP_MARK)[0] + " AS " + replicaName;
                }
                else
                {
                    columnName = replicaName;
                }
                writer.write(",\n" + columnName);
            }
            writer.write("\nFROM " + ConfigFactory.Instance().getProperty("text.table.name") + "\n");
        }
    }
}
