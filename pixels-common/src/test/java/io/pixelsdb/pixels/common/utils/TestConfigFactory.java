/*
 * Copyright 2022 PixelsDB.
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

import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

/**
 * Created at: 30/03/2022
 * Author: hank
 */
public class TestConfigFactory
{
    @Test
    public void testPropertiesURL() throws IOException
    {
        URL url = new URL("/home/hank/opt/pixels/pixels.properties");
        System.out.println(url.toString());
        InputStream in = url.openStream();
        Properties prop = new Properties();
        prop.load(in);

        System.out.println(prop.getProperty("metadata.server.host"));
    }
}
