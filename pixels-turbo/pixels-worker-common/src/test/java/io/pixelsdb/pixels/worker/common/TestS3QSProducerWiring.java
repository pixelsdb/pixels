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
import io.pixelsdb.pixels.core.PixelsWriter;
import io.pixelsdb.pixels.core.PixelsWriterImpl;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.common.utils.Constants;
import io.pixelsdb.pixels.planner.plan.physical.domain.ShuffleInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.StorageInfo;
import io.pixelsdb.pixels.storage.s3qs.S3QueueMessage;
import io.pixelsdb.pixels.storage.s3qs.S3QS;
import io.pixelsdb.pixels.storage.s3qs.PhysicalS3QSWriter;
import io.pixelsdb.pixels.storage.s3qs.S3Queue;
import org.junit.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SqsException;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Map;
import java.util.HashSet;
import java.util.concurrent.ExecutorService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

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

    @Test
    public void unpublishedWriterCannotPassPublicationCheck() throws Exception
    {
        S3QS storage = new S3QS(mock(S3Client.class), mock(SqsClient.class), 30);
        storage.registerQueue("shuffle-1", 0, "ignored", "queue-url");
        PhysicalS3QSWriter physical = (PhysicalS3QSWriter) storage.offer(
                S3QueueMessage.data("shuffle-1", 0, 0, 0, 0, "bucket/shuffle/"));
        assertThrows(IOException.class, physical::requirePublished);
    }

    @Test
    public void swallowedBufferOverflowFailsPublicationCheck() throws Exception
    {
        checkPixelsPublication(true, false);
    }

    @Test
    public void swallowedDataSendFailureFailsPublicationCheck() throws Exception
    {
        checkPixelsPublication(false, true);
    }

    @Test
    public void uploadedObjectAndPublishedDataPassPublicationCheck() throws Exception
    {
        checkPixelsPublication(false, false);
    }

    @Test
    public void producerDoesNotSendEndWhenDataWasNotPublished() throws Exception
    {
        SqsClient sqs = mock(SqsClient.class);
        S3QS storage = new S3QS(mock(S3Client.class), sqs, 30);
        storage.registerQueue("shuffle-1", 0, "ignored", "queue-url");
        // A closed local queue makes physical close return without publishing DATA.
        Field queues = S3QS.class.getDeclaredField("partitionQueues");
        queues.setAccessible(true);
        ((S3Queue) ((Map<?, ?>) queues.get(storage)).values().iterator().next()).close();
        WorkerContext context = new WorkerContext(mock(org.apache.logging.log4j.Logger.class),
                new WorkerMetrics(), "publication-check");
        try (S3QSAsyncPartitionOutput output = new S3QSAsyncPartitionOutput(
                storage, shuffleInfo(Storage.Scheme.s3qs), 0, 1, context))
        {
            VectorizedRowBatch batch = oneColumnSchema().createRowBatch();
            batch.size = 1;
            output.add(0, batch, oneColumnSchema());
            IOException failure = assertThrows(IOException.class, () -> output.finish(oneColumnSchema(), new HashSet<>()));
            assertTrue(failure.getMessage().contains("not successfully published"));
            verify(sqs, never()).sendMessage(any(SendMessageRequest.class));
        }
    }

    private static void checkPixelsPublication(boolean overflow, boolean sendFailure) throws Exception
    {
        S3Client s3 = mock(S3Client.class);
        SqsClient sqs = mock(SqsClient.class);
        if (sendFailure)
        {
            when(sqs.sendMessage(any(SendMessageRequest.class)))
                    .thenThrow(SqsException.builder().message("injected DATA publish failure").build());
        }
        S3QS storage = new S3QS(s3, sqs, 30);
        storage.registerQueue("shuffle-1", 0, "ignored", "queue-url");
        PhysicalS3QSWriter physical = (PhysicalS3QSWriter) storage.offer(
                S3QueueMessage.data("shuffle-1", 0, 0, 0, 0, "bucket/shuffle/"));
        TypeDescription schema = oneColumnSchema();
        PixelsWriter writer = PixelsWriterImpl.newBuilder().setSchema(schema)
                .setPixelStride(10000).setRowGroupSize(256 * 1024 * 1024)
                .setStorage(storage).setPath(physical.getPath()).setPhysicalWriter(physical).build();
        try
        {
            if (overflow)
            {
                // Fill the real 8 MiB stream; the row group emitted by close() cannot fit.
                physical.append(new byte[Constants.S3QS_BUFFER_SIZE]);
            }
            VectorizedRowBatch batch = schema.createRowBatch();
            batch.size = 1;
            writer.addRowBatch(batch);
            writer.close(); // Deliberately swallows both injected IOExceptions in the current common writer.
            if (overflow || sendFailure)
            {
                assertThrows(IOException.class, physical::requirePublished);
            }
            else
            {
                physical.requirePublished();
            }
            verify(s3, times(overflow ? 0 : 1)).putObject(any(PutObjectRequest.class), any(RequestBody.class));
            verify(sqs, times(overflow ? 0 : 1)).sendMessage(any(SendMessageRequest.class));
        }
        finally
        {
            // The legacy close() also skips executor shutdown on IOException; do not leak test threads.
            Field field = PixelsWriterImpl.class.getDeclaredField("columnWriterService");
            field.setAccessible(true);
            ((ExecutorService) field.get(writer)).shutdownNow();
        }
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
