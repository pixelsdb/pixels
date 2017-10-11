package cn.edu.ruc.iir.rainbow.common.util;

import org.apache.commons.logging.Log;

/**
 * Created by hank on 17-4-29.
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

    private Log log = null;

    private LogFactory ()
    {
        this.log = org.apache.commons.logging.LogFactory.getLog("oil logs");
    }

    public Log getLog ()
    {
        return this.log;
    }
}
