package cn.edu.ruc.iir.rainbow.layout.cmd;

import cn.edu.ruc.iir.rainbow.common.cmd.Command;
import cn.edu.ruc.iir.rainbow.common.cmd.Receiver;
import cn.edu.ruc.iir.rainbow.common.exception.AlgoException;
import cn.edu.ruc.iir.rainbow.common.exception.ExceptionHandler;
import cn.edu.ruc.iir.rainbow.common.exception.ExceptionType;
import cn.edu.ruc.iir.rainbow.common.exception.MetadataException;
import cn.edu.ruc.iir.rainbow.common.metadata.MetadataStat;
import cn.edu.ruc.iir.rainbow.common.metadata.OrcMetadataStat;
import cn.edu.ruc.iir.rainbow.common.metadata.ParquetMetadataStat;
import cn.edu.ruc.iir.rainbow.common.util.ConfigFactory;
import cn.edu.ruc.iir.rainbow.common.util.InputFactory;
import cn.edu.ruc.iir.rainbow.common.util.OutputFactory;
import cn.edu.ruc.iir.rainbow.layout.domian.FileFormat;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Properties;

public class CmdGetColumnSize implements Command
{
    private Receiver receiver = null;

    @Override
    public void setReceiver(Receiver receiver)
    {
        this.receiver = receiver;
    }

    /**
     * params should contain the following settings:
     * <ol>
     *   <li>schema.file,
     *   the path of the output file which will contain the column size</li>
     *   <li>file.format, orc or parquet</li>
     *   <li>hdfs.data.path, it is the directory of unordered data files on hdfs,
     *   the files should be stored as file.format</li>
     * </ol>
     *
     * this method will pass the following results to receiver:
     * <ol>
     *   <li>schema.file</li>
     *   <li>success, true or false</li>
     * </ol>
     * @param params
     */
    @Override
    public void execute(Properties params)
    {
        FileFormat format = FileFormat.valueOf(params.getProperty("file.format"));
        String schemaFilePath = params.getProperty("schema.file");
        String hdfsDataPath = params.getProperty("hdfs.table.path");
        Properties results = new Properties(params);
        results.setProperty("success", "false");

        String namenode = ConfigFactory.Instance().getProperty("namenode.host");
        int port = Integer.parseInt(ConfigFactory.Instance().getProperty("namenode.port"));

        MetadataStat stat = null;

        try
        {
            switch (format)
            {
                case ORC:
                    stat = new OrcMetadataStat(namenode, port, hdfsDataPath);
                    break;
                case PARQUET:
                    stat = new ParquetMetadataStat(namenode, port, hdfsDataPath);
                    break;
                default:
                    ExceptionHandler.Instance().log(ExceptionType.ERROR, "supported file format " + format,
                            new AlgoException("file format not supported"));
                    break;
            }
        } catch (IOException e)
        {
            ExceptionHandler.Instance().log(ExceptionType.ERROR, "I/O error when getting metadata", e);
        } catch (MetadataException e)
        {
            ExceptionHandler.Instance().log(ExceptionType.ERROR, "metadata error when getting metadata", e);
        }

        if (stat != null)
        {
            try (BufferedReader reader = InputFactory.Instance().getReader(schemaFilePath);
                 BufferedWriter writer = OutputFactory.Instance().getWriter(schemaFilePath + ".new"))
            {
                String line = null;
                double[] avgSizes = null;
                avgSizes = stat.getAvgColumnChunkSize();
                int i = 0;
                while ((line = reader.readLine()) != null)
                {
                    writer.write(line.split("\t")[0] + "\t" +
                            line.split("\t")[1] + "\t" + avgSizes[i++] + "\n");
                }
                results.setProperty("success", "true");
            } catch (IOException e)
            {
                ExceptionHandler.Instance().log(ExceptionType.ERROR, "I/O error, check the file paths", e);
            }
        }

        if (this.receiver != null)
        {
            receiver.progress(1.0);
            receiver.action(results);
        }
    }
}
