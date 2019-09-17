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
package io.pixelsdb.pixels.common.utils;

import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created at: 18-12-9
 * Author: hank
 */
public class TestHttpUtil
{
    @Test
    public void testPost() throws IOException
    {

        Map<String, String> headers = new HashMap<String, String>();
        headers.put("Content-Type", "application/x-www-form-urlencoded");
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("text",
                "Diplomacy is the art and practice of conducting negotiations between representatives of states. It usually refers to international diplomacy, the conduct of international relations[2] through the intercession of professional diplomats with regard to a full range of topical issues. International treaties are usually negotiated by diplomats prior to endorsement by national politicians. David Stevenson reports that by 1900 the term \"diplomats\" also covered diplomatic services, consular services and foreign ministry officials.");
        parameters.put("language", "en");
        parameters.put("normalize", "true");
        parameters.put("depth", "0");
        String content = HttpUtil.GetContentByPost("http://tagtheweb.com.br/wiki/getFingerPrint.php", headers, parameters);
        System.out.println(content);


    }

    @Test
    public void testGet() throws IOException
    {
        String content = HttpUtil.GetContentByGet("http://dbiir10:8080/v1/query");
        System.out.println(content);

    }
}
