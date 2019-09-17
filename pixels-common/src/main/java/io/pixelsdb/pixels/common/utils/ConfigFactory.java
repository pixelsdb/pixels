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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
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

    // Properties is thread safe, so we do not put synchronization on it.
    private Properties prop = null;

    private ConfigFactory()
    {
        prop = new Properties();
        String pixelsHome = System.getenv("PIXELS_HOME");
        InputStream in = null;
        if (pixelsHome == null)
        {
            in = this.getClass().getResourceAsStream("/pixels.properties");
        }
        else
        {
            if (!(pixelsHome.endsWith("/") || pixelsHome.endsWith("\\")))
            {
                pixelsHome += "/";
            }
            prop.setProperty("pixels.home", pixelsHome);
            try
            {
                in = new FileInputStream(pixelsHome + "pixels.properties");
            }
            catch (FileNotFoundException e)
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

    public void loadProperties(String propFilePath) throws IOException
    {
        FileInputStream in = null;
        try
        {
            in = new FileInputStream(propFilePath);
            this.prop.load(in);
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

    public void addProperty(String key, String value)
    {
        this.prop.setProperty(key, value);
    }

    public String getProperty(String key)
    {
        return this.prop.getProperty(key);
    }
}
