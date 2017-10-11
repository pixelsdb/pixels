package cn.edu.ruc.iir.rainbow.layout.cmd;

import cn.edu.ruc.iir.rainbow.common.cmd.Command;
import cn.edu.ruc.iir.rainbow.common.cmd.Receiver;
import cn.edu.ruc.iir.rainbow.common.exception.ColumnNotFoundException;
import cn.edu.ruc.iir.rainbow.common.exception.ExceptionHandler;
import cn.edu.ruc.iir.rainbow.common.exception.ExceptionType;
import cn.edu.ruc.iir.rainbow.common.util.ConfigFactory;
import cn.edu.ruc.iir.rainbow.layout.sql.GenerateQuery;

import java.io.IOException;
import java.util.Properties;

public class CmdGenerateQuery implements Command
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
     *   <li>table.name</li>
     *   <li>schema.file</li>
     *   <li>workload.file</li>
     *   <li>spark.query.file</li>
     *   <li>hive.query.file</li>
     * </ol>
     *
     * this method will pass the following results to receiver:
     * <ol>
     *   <li>spark.query.file</li>
     *   <li>hive.query.file</li>
     *   <li>success, true or false</li>
     * </ol>
     * @param params
     */
    @Override
    public void execute(Properties params)
    {
        String tableName = params.getProperty("table.name");
        String namenode = ConfigFactory.Instance().getProperty("namenode.host") + ":" +
                ConfigFactory.Instance().getProperty("namenode.port");
        String schemaFilePath = params.getProperty("schema.file");
        String workloadFilePath = params.getProperty("workload.file");
        String sparkQueryFilePath = params.getProperty("spark.query.file");
        String hiveQueryFilePath = params.getProperty("hive.query.file");

        Properties results = new Properties(params);
        results.setProperty("success", "false");
        try
        {
            GenerateQuery.Gen(tableName, namenode, schemaFilePath, workloadFilePath, sparkQueryFilePath, hiveQueryFilePath);
            results.setProperty("success", "true");
        } catch (IOException e)
        {
            ExceptionHandler.Instance().log(ExceptionType.ERROR, "I/O error, check the file paths", e);
        } catch (ColumnNotFoundException e)
        {
            ExceptionHandler.Instance().log(ExceptionType.ERROR, "column not fount when generating queries", e);
        }

        if (this.receiver != null)
        {
            receiver.progress(1.0);
            receiver.action(results);
        }
    }
}
