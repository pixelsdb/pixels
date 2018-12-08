package cn.edu.ruc.iir.pixels.listener;

import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.spi.eventlistener.EventListenerFactory;
import io.airlift.log.Logger;

import java.util.Map;

/**
 * Created at: 18-12-8
 * Author: hank
 */
public class PixelsEventListenerFactory implements EventListenerFactory
{
    private Logger logger = Logger.get(PixelsEventListenerFactory.class);

    private final String name = "pixels-event-listener";

    @Override
    public String getName()
    {
        return this.name;
    }

    @Override
    public EventListener create(Map<String, String> config)
    {
        String logDir = config.get("log.dir");
        if (logDir == null)
        {
            logger.error("log.dir for pixels-event-listener is not defined.");
            return null;
        }
        boolean enabled = Boolean.parseBoolean(config.getOrDefault("enabled", "true"));
        return new PixelsEventListener(logDir, enabled);
    }
}
