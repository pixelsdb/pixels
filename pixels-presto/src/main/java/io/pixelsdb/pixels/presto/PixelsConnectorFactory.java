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
package io.pixelsdb.pixels.presto;

import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.airlift.log.Logger;
import io.pixelsdb.pixels.presto.exception.PixelsErrorCode;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class PixelsConnectorFactory
        implements ConnectorFactory {

    private Logger logger = Logger.get(PixelsConnectorFactory.class);

    private final String name = "pixels";

    public PixelsConnectorFactory()  {
        logger.debug("Connector " + name + " initialized.");
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public ConnectorHandleResolver getHandleResolver() {
        return new PixelsHandleResolver();
    }

    @Override
    public Connector create(final String connectorId, Map<String, String> requiredConfig, ConnectorContext context) {
        requireNonNull(requiredConfig, "requiredConfig is null");
        try {
            // A plugin is not required to use Guice; it is just very convenient
            Bootstrap app = new Bootstrap(
                    new JsonModule(),
                    new PixelsModule(connectorId, context.getTypeManager()));

            Injector injector = app
                    .strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(requiredConfig)
                    .initialize();

            return injector.getInstance(PixelsConnector.class);
        } catch (Exception e) {
            throw new PrestoException(PixelsErrorCode.PIXELS_CONNECTOR_ERROR, e);
        }
    }
}
