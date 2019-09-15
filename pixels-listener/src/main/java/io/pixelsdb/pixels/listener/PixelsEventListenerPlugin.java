package io.pixelsdb.pixels.listener;

import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.eventlistener.EventListenerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created at: 18-12-9
 * Author: hank
 */
public class PixelsEventListenerPlugin implements Plugin
{
    @Override
    public Iterable<EventListenerFactory> getEventListenerFactories()
    {
        EventListenerFactory listenerFactory = new PixelsEventListenerFactory();
        List<EventListenerFactory> listenerFactoryList = new ArrayList<>();
        listenerFactoryList.add(listenerFactory);
        List<EventListenerFactory> immutableList = Collections.unmodifiableList(listenerFactoryList);

        return immutableList;
    }
}
