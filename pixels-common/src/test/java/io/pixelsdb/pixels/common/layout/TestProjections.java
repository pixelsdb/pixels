/*
 * Copyright 2021 PixelsDB.
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
package io.pixelsdb.pixels.common.layout;

import com.alibaba.fastjson.JSON;
import io.pixelsdb.pixels.common.metadata.domain.OriginProjectionPattern;
import io.pixelsdb.pixels.common.metadata.domain.Projections;
import org.junit.Test;

import java.util.Arrays;

/**
 * Created at: 10/20/21
 * Author: hank
 */
public class TestProjections
{
    @Test
    public void testProjectionsToJson()
    {
        Projections projections = new Projections();
        projections.setNumProjections(1);
        OriginProjectionPattern projectionPattern = new OriginProjectionPattern();
        projectionPattern.setPathIds(0);
        for (int i = 0; i < 1187; ++i)
        {
            projectionPattern.addAccessedColumns(i);
        }
        projections.addProjectionPatterns(projectionPattern);
        System.out.println(JSON.toJSONString(projections));
    }

    @Test
    public void testJsonToProjections()
    {
        String str = "{\"numProjections\":1,\"projectionPatterns\":[{\"accessedColumns\":" +
                "[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19],\"pathIds\":[0]}]}";
        Projections projections = JSON.parseObject(str, Projections.class);
        System.out.println(projections.getNumProjections());
        for (OriginProjectionPattern pattern : projections.getProjectionPatterns())
        {
            System.out.println(pattern.getAccessedColumns());
            System.out.println(Arrays.toString(pattern.getPathIds()));
        }
    }
}
