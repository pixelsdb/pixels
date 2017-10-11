package cn.edu.ruc.iir.rainbow.layout.cmd;

import cn.edu.ruc.iir.rainbow.common.cmd.Command;
import cn.edu.ruc.iir.rainbow.common.cmd.Receiver;
import cn.edu.ruc.iir.rainbow.common.exception.ExceptionHandler;
import cn.edu.ruc.iir.rainbow.common.exception.ExceptionType;
import cn.edu.ruc.iir.rainbow.layout.sql.GenerateLoad;

import java.io.IOException;
import java.util.Properties;

public class CmdGenerateLoad implements Command
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
     *   <li>overwrite, true for false</li>
     *   <li>table.name</li>
     *   <li>schema.file</li>
     *   <li>load.file</li>
     * </ol>
     *
     * this method will pass the following results to receiver:
     * <ol>
     *   <li>load.file</li>
     *   <li>success, true or false</li>
     * </ol>
     * @param params
     */
    @Override
    public void execute(Properties params)
    {
        boolean overwrite = Boolean.parseBoolean(params.getProperty("overwrite", "false"));
        String schemaFilePath = params.getProperty("schema.file");
        String loadFilePath = params.getProperty("load.file");
        String tableName = params.getProperty("table.name");
        Properties results = new Properties(params);
        results.setProperty("success", "false");
        try
        {
            GenerateLoad.Gen(overwrite, tableName, schemaFilePath, loadFilePath);
            results.setProperty("success", "true");
        } catch (IOException e)
        {
            ExceptionHandler.Instance().log(ExceptionType.ERROR, "I/O error, check the file paths", e);
        }

        if (this.receiver != null)
        {
            receiver.progress(1.0);
            receiver.action(results);
        }
    }
}
