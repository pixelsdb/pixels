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
package io.pixelsdb.pixels.lambda;

import com.alibaba.fastjson.JSON;
import com.google.gson.Gson;
import io.pixelsdb.pixels.core.predicate.ColumnFilter;
import org.junit.Test;

/**
 * Created at: 07/04/2022
 * Author: hank
 */
public class TestColumnFilter
{
    @Test
    public void testParsing()
    {
        long start = System.currentTimeMillis();
        String json = "{\"columnName\":\"id\",\"columnType\":\"LONG\",\"filterJson\":\"{\\\"javaType\\\":\\\"long\\\",\\\"lowerBounds\\\":[{\\\"type\\\":\\\"UNBOUNDED\\\"},{\\\"type\\\":\\\"EXCLUDED\\\",\\\"value\\\":200}],\\\"upperBounds\\\":[{\\\"type\\\":\\\"INCLUDED\\\",\\\"value\\\":100},{\\\"type\\\":\\\"UNBOUNDED\\\"}],\\\"singleValues\\\":[{\\\"type\\\":\\\"INCLUDED\\\",\\\"value\\\":150}]}\"}\n";
        ColumnFilter filter = new Gson().fromJson(json, ColumnFilter.class);
        System.out.println(System.currentTimeMillis()-start);
        ColumnFilter filter0 = JSON.parseObject(json, ColumnFilter.class);
        System.out.println(System.currentTimeMillis()-start);
    }
}
