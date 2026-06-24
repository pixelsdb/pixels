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
 * You should have received a copy of the GNU Affero General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.worker.common;

import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.executor.join.Joiner;
import io.pixelsdb.pixels.planner.plan.physical.domain.PartitionInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.PartitionedTableInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.ShuffleInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.StorageInfo;
import io.pixelsdb.pixels.storage.s3qs.S3QueuePollResult;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestS3QSConsumerWiring
{
    @Test
    public void s3qsConsumerPathRequiresExplicitShuffleInfo()
    {
        PartitionedTableInfo noShuffle = tableInfo(Storage.Scheme.s3qs, null);
        assertFalse(BasePartitionedJoinWorker.isS3QSShuffle(noShuffle));

        PartitionedTableInfo s3Shuffle = tableInfo(Storage.Scheme.s3qs, shuffleInfo(Storage.Scheme.s3));
        assertFalse(BasePartitionedJoinWorker.isS3QSShuffle(s3Shuffle));

        PartitionedTableInfo s3qsShuffle = tableInfo(Storage.Scheme.s3, shuffleInfo(Storage.Scheme.s3qs));
        assertTrue(BasePartitionedJoinWorker.isS3QSShuffle(s3qsShuffle));
    }

    @Test
    public void s3qsProbeHelpersAreNoOpWithoutAssignedPartitions() throws Exception
    {
        assertEquals(0, BasePartitionedJoinWorker.joinWithRightTableS3QS(
                1L, 1L, null, null, new HashMap<Integer, Queue<S3QueuePollResult>>(),
                new String[0], Collections.emptyList(), new ConcurrentLinkedQueue<>(), new WorkerMetrics()));

        assertEquals(0, BasePartitionedChainJoinWorker.joinWithRightTableS3QS(
                1L, 1L, null, null, null, new HashMap<Integer, Queue<S3QueuePollResult>>(),
                new String[0], Collections.emptyList(), new ConcurrentLinkedQueue<>(), new WorkerMetrics()));
    }

    @Test
    public void s3qsPostPartitionHelpersAreNoOpWithoutAssignedPartitions() throws Exception
    {
        PartitionInfo postPartition = new PartitionInfo(new int[] {0}, 1);
        Joiner joiner = mockJoinerWithJoinedSchema();

        assertEquals(0, BasePartitionedJoinWorker.joinWithRightTableAndPartitionS3QS(
                1L, 1L, joiner, null, new HashMap<Integer, Queue<S3QueuePollResult>>(),
                new String[0], Collections.emptyList(), postPartition,
                Collections.singletonList(new ConcurrentLinkedQueue<>()), new WorkerMetrics()));

        assertEquals(0, BasePartitionedChainJoinWorker.joinWithRightTableAndPartitionS3QS(
                1L, 1L, null, joiner, null, new HashMap<Integer, Queue<S3QueuePollResult>>(),
                new String[0], Collections.emptyList(), postPartition,
                Collections.singletonList(new ConcurrentLinkedQueue<>()), new WorkerMetrics()));
    }

    private static Joiner mockJoinerWithJoinedSchema()
    {
        Joiner joiner = mock(Joiner.class);
        TypeDescription joinedSchema = TypeDescription.createStruct()
                .addField("key", TypeDescription.createLong());
        when(joiner.getJoinedSchema()).thenReturn(joinedSchema);
        return joiner;
    }

    private static PartitionedTableInfo tableInfo(Storage.Scheme tableScheme, ShuffleInfo shuffleInfo)
    {
        PartitionedTableInfo tableInfo = new PartitionedTableInfo();
        tableInfo.setStorageInfo(new StorageInfo(tableScheme, null, null, null, null));
        tableInfo.setShuffleInfo(shuffleInfo);
        return tableInfo;
    }

    private static ShuffleInfo shuffleInfo(Storage.Scheme shuffleScheme)
    {
        return new ShuffleInfo("shuffle-1", new StorageInfo(shuffleScheme, null, null, null, null),
                "s3://bucket/shuffle-1/", 1, 1, 1, 1, Collections.emptyList());
    }
}
