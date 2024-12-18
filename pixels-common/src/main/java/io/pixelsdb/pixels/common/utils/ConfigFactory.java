/*
 * Copyright 2017 PixelsDB.
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
package io.pixelsdb.pixels.common.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author hank
 */
public class ConfigFactory
{
    private static ConfigFactory instance = null;

    /**
     * Injected classes in pixels-presto should not use this method to get ConfigFactory Instance.
     *
     * @return
     */
    public static ConfigFactory Instance()
    {
        if (instance == null)
        {
            instance = new ConfigFactory();
        }
        return instance;
    }

    // Properties is thread safe, so we do not add synchronization to it.
    private final Properties prop;

    /**
     * Issue #114:
     * Add configuration update callbacks.
     * By registering update callback, other class can get their
     * members updated when the properties on ConfigurationFactory
     * are reloaded or updated.
     */
    public interface UpdateCallback
    {
        void update(String value);
    }

    private Map<String, UpdateCallback> callbacks;

    private ConfigFactory()
    {
        prop = new Properties();
        callbacks = new HashMap<>();
        // firstly try to get the config file path from PIXELS_CONFIG.
        String pixelsConfig = System.getenv("PIXELS_CONFIG");
        String pixelsHome = System.getenv("PIXELS_HOME");
        if (pixelsHome != null)
        {
            prop.setProperty("pixels.home", pixelsHome);
        }
        if (pixelsConfig == null && pixelsHome != null)
        {
            // try to get the config file under PIXELS_HOME if PIXELS_CONFIG is not set.
            if (!(pixelsHome.endsWith("/") || pixelsHome.endsWith("\\")))
            {
                pixelsHome += "/";
            }
            pixelsConfig = pixelsHome + "etc/pixels.properties";
        }

        InputStream in = null;
        if (pixelsConfig == null)
        {
            // neither PIXELS_CONFIG nor PIXELS_HOME is found, use the config file under CLASS_PATH.
            in = this.getClass().getResourceAsStream("/pixels.properties");
        }
        else
        {
            try
            {
                /**
                 * Issue #245:
                 * I have not found a simple and graceful way to validate a URL string.
                 * Catching the exception from new URL() is equivalent to startWith().
                 *
                 * Currently, we do not support other protocols such as ftp.
                 */
                if (pixelsConfig.startsWith("https://") || pixelsConfig.startsWith("http://"))
                {
                    URL url = new URL(pixelsConfig);
                    in = url.openStream();
                }
                else
                {
                    in = new FileInputStream(pixelsConfig);
                }
            } catch (IOException e)
            {
                e.printStackTrace();
            }
        }
        try
        {
            if (in != null)
            {
                prop.load(in);
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    public synchronized void registerUpdateCallback(String key, UpdateCallback callback)
    {
        this.callbacks.put(key, callback);
    }

    public synchronized void loadProperties(String propFilePath) throws IOException
    {
        InputStream in = null;
        try
        {
            /**
             * Issue #245:
             * Refer to the comments in the above constructor.
             * Currently, we do not support other protocols such as ftp.
             */
            if (propFilePath.startsWith("https://") || propFilePath.startsWith("http://"))
            {
                URL url = new URL(propFilePath);
                in = url.openStream();
            }
            else
            {
                in = new FileInputStream(propFilePath);
            }
            this.prop.load(in);
            for (Map.Entry<String, UpdateCallback> entry : this.callbacks.entrySet())
            {
                String value = this.prop.getProperty(entry.getKey());
                if (value != null)
                {
                    entry.getValue().update(value);
                }
            }
        }
        catch (IOException e)
        {
            throw e;
        }
        finally
        {
            if (in != null)
            {
                try
                {
                    in.close();
                }
                catch (IOException e)
                {
                    throw e;
                }
            }
        }
    }

    public synchronized void addProperty(String key, String value)
    {
        this.prop.setProperty(key, value);
        if (this.callbacks.containsKey(key))
        {
            this.callbacks.get(key).update(value);
        }
    }

    public synchronized String getProperty(String key)
    {
        return this.prop.getProperty(key);
    }
}
