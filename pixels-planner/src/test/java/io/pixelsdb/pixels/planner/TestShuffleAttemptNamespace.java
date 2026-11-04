/*
 * Copyright 2026 PixelsDB.
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
package io.pixelsdb.pixels.planner;

import io.pixelsdb.pixels.planner.plan.logical.Table;
import io.pixelsdb.pixels.planner.plan.physical.domain.ShuffleInfo;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class TestShuffleAttemptNamespace
{
    @Test
    public void replanningTheSameTransactionCannotReuseOwnedShuffleResources()
    {
        Table table = mock(Table.class);
        when(table.getSchemaName()).thenReturn("schema");
        when(table.getTableName()).thenReturn("table");
        ShuffleInfo first = PixelsPlanner.createShuffleInfo(1277, table, "bucket/query/table/", 2, 3);
        ShuffleInfo next = PixelsPlanner.createShuffleInfo(1277, table, "bucket/query/table/", 2, 3);
        assertNotEquals(first.getShuffleId(), next.getShuffleId());
        assertNotEquals(first.getObjectPathPrefix(), next.getObjectPathPrefix());
        assertFalse(next.getObjectPathPrefix().startsWith(first.getObjectPathPrefix()));
        assertTrue(first.getObjectPathPrefix().startsWith("bucket/query/table/s3qs-"));
        assertTrue(first.getObjectPathPrefix().endsWith("/"));
        for (int i = 0; i < 2; i++)
        {
            assertNotEquals(first.getQueues().get(i).getQueueName(), next.getQueues().get(i).getQueueName());
            assertTrue(first.getQueues().get(i).getQueueName().length() <= 80);
        }
        assertEquals(3, first.getLogicalProducerTaskCount());
    }
}
