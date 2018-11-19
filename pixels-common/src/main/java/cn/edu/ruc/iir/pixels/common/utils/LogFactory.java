package cn.edu.ruc.iir.pixels.common.utils;

import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by hank on 18-1-25.
 */
public class LogFactory
{
    private static LogFactory instance = null;
    public static LogFactory Instance ()
    {
        if (instance == null)
        {
            instance = new LogFactory();
        }
        return instance;
    }

    private Logger log = null;

    private LogFactory ()
    {
        String logPath = ConfigFactory.Instance().getProperty("LOG.PATH");
        File path = new File(logPath);
        if (!path.exists()) {
            boolean log = path.mkdirs();
            System.out.println(log == true ? "Create log dir." : "Failed to create log dir.");
            System.out.println("Log dir: " + logPath);
        }
        this.log = Logger.getLogger(LogFactory.class.getName());
    }

    public Logger getLog ()
    {
        return this.log;
    }
}
