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
        String prefix = config.get("listened.user.prefix");
        String schema = config.get("listened.schema");
        String queryType = config.get("listened.query.type");
        if (logDir == null)
        {
            logger.error("log.dir for pixels-event-listener is not defined.");
            return null;
        }
        boolean enabled = Boolean.parseBoolean(config.getOrDefault("enabled", "true"));
        return new PixelsEventListener(logDir, enabled, prefix, schema, queryType);
    }
}
