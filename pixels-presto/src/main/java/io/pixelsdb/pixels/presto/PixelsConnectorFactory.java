/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

    private final String name = "pixels-presto";

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
