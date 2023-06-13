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
package io.pixelsdb.pixels.common;

import com.alibaba.fastjson.JSON;
import io.pixelsdb.pixels.common.metadata.domain.Projections;
import org.junit.Test;

import java.util.Random;

/**
 * Created at: 18-12-23
 * Author: hank
 */
public class TestRandom
{
    @Test
    public void test()
    {
        Random random = new Random(System.nanoTime());
        System.out.println(random.nextInt(1));

        String str = "file:///data1/pixels;file:///data2/pixels";
        for (String split : str.split(";"))
        {
            System.out.println(split);
        }
        System.out.println();

        str = "file:///data3/pixels";
        for (String split : str.split(";"))
        {
            System.out.println(split);
        }

        Projections projections = new Projections();
        System.out.println(JSON.toJSONString(projections));
        System.out.println(JSON.parseObject("", Projections.class) == null);
    }
}
