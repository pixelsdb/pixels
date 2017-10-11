package cn.edu.ruc.iir.rainbow.common.exception;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Created by hank on 12/15/16.
 */
public class ExceptionHandler
{
    private static ExceptionHandler instance = null;

    public static ExceptionHandler Instance ()
    {
        if (instance == null)
        {
            instance = new ExceptionHandler();
        }
        return instance;
    }

    private ExceptionHandler () {}

    private Log logger = LogFactory.getLog(this.getClass());

    public void log (ExceptionType type, String message, Exception e)
    {
        if (type == null)
        {
            logger.error(message, e);
            return;
        }
        switch (type)
        {
            case ERROR:
                logger.error(message, e);
                break;
            case DEBUG:
                logger.debug(message, e);
                break;
            case INFO:
                logger.info(message, e);
                break;
            case FATAL:
                logger.fatal(message, e);
                break;
            case WARN:
                logger.warn(message, e);
                break;
            default:
                logger.error(message, e);
                break;
        }
    }

}
