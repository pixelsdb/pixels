package cn.edu.ruc.iir.pixels.listener;

import cn.edu.ruc.iir.pixels.common.utils.DateUtil;
import cn.edu.ruc.iir.pixels.listener.exception.ListenerExecption;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;
import com.facebook.presto.spi.eventlistener.QueryCreatedEvent;
import com.facebook.presto.spi.eventlistener.SplitCompletedEvent;
import io.airlift.log.Logger;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import static cn.edu.ruc.iir.pixels.listener.exception.ListenerErrorCode.PIXELS_EVENT_LISTENER_ERROR;

/**
 * Created at: 18-12-8
 * Author: hank
 */
public class PixelsEventListener implements EventListener
{
    private Logger logger = Logger.get(PixelsEventListener.class);

    private final String logDir;
    private final boolean enabled;
    private static BufferedWriter LogWriter = null;

    public PixelsEventListener (String logDir, boolean enabled)
    {
        this.logDir = logDir.endsWith("/") ? logDir : logDir + "/";
        this.enabled = enabled;
        try
        {
            if (this.enabled == true && LogWriter == null)
            {
                LogWriter = new BufferedWriter(new FileWriter(
                                this.logDir + "pixels_query_" +
                                        DateUtil.getCurTime() + ".log"));
                LogWriter.write("\"query id\",\"user\",\"uri\"");
                LogWriter.newLine();
                LogWriter.flush();
            }
        } catch (IOException e)
        {
            throw new PrestoException(PIXELS_EVENT_LISTENER_ERROR,
                    new ListenerExecption("can not create log writer."));
        }
    }

    @Override
    public void queryCreated(QueryCreatedEvent queryCreatedEvent)
    {
    }

    @Override
    public void queryCompleted(QueryCompletedEvent queryCompletedEvent)
    {
        if (this.enabled == false)
        {
            return;
        }

        String queryId = queryCompletedEvent.getMetadata().getQueryId();
        String user = queryCompletedEvent.getContext().getUser();
        String uri = queryCompletedEvent.getMetadata().getUri().toString();
        try
        {
            LogWriter.write(queryId + "," + user + "," + uri);
            LogWriter.newLine();
            LogWriter.flush();
        } catch (IOException e)
        {
            logger.error("can not write log in pixels event listener.");
            logger.info("query id: " + queryId + ", user: " + user + ", uri: " + uri);
        }
    }

    @Override
    public void splitCompleted(SplitCompletedEvent splitCompletedEvent)
    {
    }
}
