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
package io.pixelsdb.pixels.presto;

import io.pixelsdb.pixels.common.metadata.domain.Column;
import com.alibaba.fastjson.JSON;
import org.junit.Test;

public class TestJson
{
    @Test
    public void test ()
    {
        Column column = new Column();
        column.setId(1);
        //column.setName("c1");
        column.setType("int");
        System.out.println(JSON.toJSONString(column));
    }
}
