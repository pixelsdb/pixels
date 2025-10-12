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

import org.apache.http.Consts;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author hank
 */
public class HttpUtil
{
    /**
     * Get the content from baseUrl by post method with given headers and parameters.
     * @param baseUrl
     * @param headers
     * @param parameters
     * @return
     * @throws IOException
     */
    public static String GetContentByPost(String baseUrl, Map<String, String> headers, Map<String, String> parameters) throws IOException
    {
        try (CloseableHttpClient httpClient = HttpClients.createDefault())
        {
            HttpPost httpPost = new HttpPost(baseUrl);

            for (Map.Entry<String, String> header : headers.entrySet())
            {
                httpPost.setHeader(header.getKey(), header.getValue());
            }

            List<NameValuePair> nvps = new ArrayList<>();
            for (Map.Entry<String, String> parameter : parameters.entrySet())
            {
                nvps.add(new BasicNameValuePair(parameter.getKey(), parameter.getValue()));
            }

            httpPost.setEntity(new UrlEncodedFormEntity(nvps, Consts.UTF_8));
            try (CloseableHttpResponse response = httpClient.execute(httpPost))
            {
                HttpEntity entity = response.getEntity();
                return EntityUtils.toString(entity);
            }
        }
    }

    /**
     * Get the content from url by get method.
     * @param url
     * @return
     * @throws IOException
     */
    public static String GetContentByGet(String url) throws IOException
    {
        try (CloseableHttpClient httpClient = HttpClients.createDefault())
        {
            HttpGet httpGet = new HttpGet(url);
            try (CloseableHttpResponse response = httpClient.execute(httpGet))
            {
                HttpEntity entity = response.getEntity();
                return EntityUtils.toString(entity);
            }
        }
    }
}
