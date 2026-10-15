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
package io.pixelsdb.pixels.worker.common;

import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.planner.plan.physical.domain.ShuffleInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.StorageInfo;
import io.pixelsdb.pixels.storage.s3qs.S3QueueMessage;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestS3QSProducerWiring
{
    @Test
    public void s3qsProducerPathRequiresExplicitShuffleInfo()
    {
        assertFalse(BasePartitionWorker.isS3QSShuffle(null));
        assertFalse(BasePartitionWorker.isS3QSShuffle(shuffleInfo(Storage.Scheme.s3)));
        assertTrue(BasePartitionWorker.isS3QSShuffle(shuffleInfo(Storage.Scheme.s3qs)));
    }

    @Test
    public void dataMessageCarriesProducerPartitionAndSchemaMetadata() throws Exception
    {
        TypeDescription schema = oneColumnSchema();
        S3QueueMessage message = BasePartitionWorker.createS3QSDataMessage(
                shuffleInfo(Storage.Scheme.s3qs), 3, 7, 0, 11L, schema);

        assertTrue(message.isData());
        assertFalse(message.isProducerEnd());
        assertEquals("shuffle-1", message.getShuffleId());
        assertEquals(3, message.getPartitionId());
        assertEquals(7, message.getProducerId());
        assertEquals(0, message.getProducerAttemptId());
        assertEquals(11L, message.getSequenceId());
        assertEquals("s3://bucket/shuffle-1/", message.getObjectPath());
        assertEquals(schema.toString(), message.getMetadata());

        S3QueueMessage parsed = S3QueueMessage.fromMessageBody(message.toMessageBody());
        assertTrue(parsed.isData());
        assertEquals(message.getShuffleId(), parsed.getShuffleId());
        assertEquals(message.getPartitionId(), parsed.getPartitionId());
        assertEquals(message.getProducerId(), parsed.getProducerId());
        assertEquals(message.getSequenceId(), parsed.getSequenceId());
        assertEquals(message.getObjectPath(), parsed.getObjectPath());
        assertEquals(message.getMetadata(), parsed.getMetadata());
    }

    @Test
    public void producerEndMessageUsesSameProducerIdentityAndNormalEndReason() throws Exception
    {
        TypeDescription schema = oneColumnSchema();
        S3QueueMessage message = BasePartitionWorker.createS3QSProducerEndMessage(
                shuffleInfo(Storage.Scheme.s3qs), 5, 9, 0, 2L, schema);

        assertFalse(message.isData());
        assertTrue(message.isProducerEnd());
        assertEquals("shuffle-1", message.getShuffleId());
        assertEquals(5, message.getPartitionId());
        assertEquals(9, message.getProducerId());
        assertEquals(0, message.getProducerAttemptId());
        assertEquals(2L, message.getSequenceId());
        assertEquals("NORMAL", message.getEndReason());
        assertEquals("", message.getObjectPath());
        assertEquals(schema.toString(), message.getMetadata());

        S3QueueMessage parsed = S3QueueMessage.fromMessageBody(message.toMessageBody());
        assertTrue(parsed.isProducerEnd());
        assertEquals(message.getShuffleId(), parsed.getShuffleId());
        assertEquals(message.getPartitionId(), parsed.getPartitionId());
        assertEquals(message.getProducerId(), parsed.getProducerId());
        assertEquals(message.getSequenceId(), parsed.getSequenceId());
        assertEquals(message.getEndReason(), parsed.getEndReason());
        assertEquals(message.getMetadata(), parsed.getMetadata());
    }

    private static TypeDescription oneColumnSchema()
    {
        return TypeDescription.createStruct()
                .addField("key", TypeDescription.createLong());
    }

    private static ShuffleInfo shuffleInfo(Storage.Scheme shuffleScheme)
    {
        return new ShuffleInfo("shuffle-1", new StorageInfo(shuffleScheme, null, null, null, null),
                "s3://bucket/shuffle-1/", 8, 2, 4, 1, Collections.emptyList());
    }
}
