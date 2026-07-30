/*
 * Copyright 2026 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of the License,
 * or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public License
 * along with Pixels.  If not, see <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.planner.plan.physical.domain;

import com.alibaba.fastjson.JSON;
import io.pixelsdb.pixels.common.physical.Storage;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestShuffleInfo
{
    @Test
    public void serializesLogicalProducerTaskCountWithoutWorkerAmbiguity()
    {
        ShuffleInfo shuffleInfo = shuffleInfo(7);

        String json = JSON.toJSONString(shuffleInfo);

        assertTrue(json.contains("\"logicalProducerTaskCount\":7"));
        assertFalse(json.contains("\"producerCount\""));
        assertEquals(7, JSON.parseObject(json, ShuffleInfo.class).getLogicalProducerTaskCount());
    }

    private static ShuffleInfo shuffleInfo(int logicalProducerTaskCount)
    {
        return new ShuffleInfo("shuffle-1277",
                new StorageInfo(Storage.Scheme.s3qs, null, null, null, null),
                "bucket/query/shuffle-1277/", 2, logicalProducerTaskCount, 2, 20,
                Collections.singletonList(new ShuffleQueueInfo(0, "shuffle-1277-p0", null)));
    }
}
