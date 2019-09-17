/*
 * Copyright 2018 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
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
