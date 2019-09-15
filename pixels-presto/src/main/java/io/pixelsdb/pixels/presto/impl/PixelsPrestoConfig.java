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
 * License along with Foobar.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.presto.impl;

import io.pixelsdb.pixels.common.exception.FSException;
import io.pixelsdb.pixels.common.physical.FSFactory;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import com.facebook.presto.spi.PrestoException;
import io.airlift.configuration.Config;
import io.airlift.log.Logger;
import io.pixelsdb.pixels.presto.exception.PixelsErrorCode;

import javax.validation.constraints.NotNull;
import java.io.IOException;

/**
 * @Description: Configuration read from etc/catalog/pixels-presto.properties
 * @author: tao
 * @date: Create in 2018-01-20 11:16
 **/
public class PixelsPrestoConfig
{
    private Logger logger = Logger.get(PixelsPrestoConfig.class);
    private ConfigFactory configFactory = null;
    private FSFactory fsFactory = null;

    private static int BatchSize = 1000;

    public static int getBatchSize()
    {
        return BatchSize;
    }

    private String pixelsHome = null;

    @Config("pixels.home")
    public PixelsPrestoConfig setPixelsHome (String pixelsHome)
    {
        this.pixelsHome = pixelsHome;

        // reload configuration
        if (this.configFactory == null)
        {
            if (pixelsHome == null || pixelsHome.isEmpty())
            {
                String defaultPixelsHome = ConfigFactory.Instance().getProperty("pixels.home");
                if (defaultPixelsHome == null)
                {
                    logger.info("using pixels.properties insided in jar.");
                } else
                {
                    logger.info("using pixels.properties under default pixels.home: " + defaultPixelsHome);
                }
            } else
            {
                if (!(pixelsHome.endsWith("/") || pixelsHome.endsWith("\\")))
                {
                    pixelsHome += "/";
                }
                try
                {
                    ConfigFactory.Instance().loadProperties(pixelsHome + "pixels.properties");
                    ConfigFactory.Instance().addProperty("pixels.home", pixelsHome);
                    logger.info("using pixels.properties under connector specified pixels.home: " + pixelsHome);

                } catch (IOException e)
                {
                    logger.error(e,"can not load pixels.properties under: " + pixelsHome +
                            ", configuration reloading is skipped.");
                    throw new PrestoException(PixelsErrorCode.PIXELS_CONFIG_ERROR, e);
                }
            }

            this.configFactory = ConfigFactory.Instance();
            try
            {
                this.fsFactory = FSFactory.Instance(this.configFactory.getProperty("hdfs.config.dir"));

            } catch (FSException e)
            {
                throw new PrestoException(PixelsErrorCode.PIXELS_CONFIG_ERROR, e);
            }
            try
            {
                int batchSize = Integer.parseInt(this.configFactory.getProperty("row.batch.size"));
                if (batchSize > 0)
                {
                    BatchSize = batchSize;
                    logger.info("using pixels row.batch.size: " + BatchSize);
                }
            } catch (NumberFormatException e)
            {
                throw new PrestoException(PixelsErrorCode.PIXELS_CONFIG_ERROR, e);
            }
        }
        return this;
    }

    @NotNull
    public String getPixelsHome ()
    {
        return this.pixelsHome;
    }

    /**
     * Injected class should get ConfigFactory instance by this method instead of ConfigFactory.Instance().
     * @return
     */
    @NotNull
    public ConfigFactory getConfigFactory()
    {
        return this.configFactory;
    }

    /**
     * Injected class should get FSFactory instance by this method instead of FSFactory.Instance(...).
     * @return
     */
    @NotNull
    public FSFactory getFsFactory()
    {
        return this.fsFactory;
    }
}
