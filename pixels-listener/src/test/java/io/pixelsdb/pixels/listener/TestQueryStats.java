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
package io.pixelsdb.pixels.listener;

import io.pixelsdb.pixels.common.utils.HttpUtil;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.ParserConfig;
import org.junit.Test;

import java.io.IOException;

/**
 * Created at: 18-12-9
 * Author: hank
 */
public class TestQueryStats
{
    static
    {
        ParserConfig.getGlobalInstance().setAutoTypeSupport(true);
    }

    @Test
    public void test () throws IOException
    {
        String content = HttpUtil.GetContentByGet("http://dbiir10:8080/v1/query/20181209_062931_00226_4ahrk");
        JSONObject object = JSONObject.parseObject(content);
        String str = object.getJSONObject("queryStats").getString("elapsedTime");
        if (str.endsWith("ms"))
        {
            System.out.println(str.substring(0, str.indexOf("ms")));
        }
        else if (str.endsWith("s"))
        {
            System.out.println(str.substring(0, str.indexOf("s")));
        }
        else if (str.endsWith("m"))
        {
            System.out.println(str.substring(0, str.indexOf("m")));
        }
        else if (str.endsWith("h"))
        {
            System.out.println(str.substring(0, str.indexOf("h")));
        }
    }
}
