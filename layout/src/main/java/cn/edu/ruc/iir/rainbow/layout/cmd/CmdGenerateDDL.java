package cn.edu.ruc.iir.rainbow.layout.cmd;

import cn.edu.ruc.iir.rainbow.common.cmd.Command;
import cn.edu.ruc.iir.rainbow.common.cmd.Receiver;
import cn.edu.ruc.iir.rainbow.common.exception.AlgoException;
import cn.edu.ruc.iir.rainbow.common.exception.ExceptionHandler;
import cn.edu.ruc.iir.rainbow.common.exception.ExceptionType;
import cn.edu.ruc.iir.rainbow.layout.domian.FileFormat;
import cn.edu.ruc.iir.rainbow.layout.sql.GenerateDDL;

import java.io.IOException;
import java.util.Properties;

public class CmdGenerateDDL implements Command
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
     *   <li>file.format, orc, parquet, text</li>
     *   <li>table.name</li>
     *   <li>schema.file</li>
     *   <li>ddl.file</li>
     * </ol>
     *
     * this method will pass the following results to receiver:
     * <ol>
     *   <li>ddl.file</li>
     *   <li>success, true or false</li>
     * </ol>
     * @param params
     */
    @Override
    public void execute(Properties params)
    {
        FileFormat format = FileFormat.valueOf(params.getProperty("file.format"));
        String schemaFilePath = params.getProperty("schema.file");
        String ddlFilePath = params.getProperty("ddl.file");
        String tableName = params.getProperty("table.name");
        Properties results = new Properties(params);
        results.setProperty("success", "false");
        try
        {
            switch (format)
            {
                case ORC:
                    GenerateDDL.GenCreateOrc(tableName, schemaFilePath, ddlFilePath);
                    results.setProperty("success", "true");
                    break;
                case PARQUET:
                    GenerateDDL.GenCreateParq(tableName, schemaFilePath, ddlFilePath);
                    results.setProperty("success", "true");
                    break;
                case TEXT:
                    GenerateDDL.GenCreateText(schemaFilePath, ddlFilePath);
                    results.setProperty("success", "true");
                    break;
                default:
                    ExceptionHandler.Instance().log(ExceptionType.ERROR, "unknown file format " + format,
                            new AlgoException("file format not supported"));
                    break;
            }
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
