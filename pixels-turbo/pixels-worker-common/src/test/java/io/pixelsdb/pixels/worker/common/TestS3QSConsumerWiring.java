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
import io.pixelsdb.pixels.common.physical.PhysicalReader;
import io.pixelsdb.pixels.common.physical.PhysicalReaderUtil;
import io.pixelsdb.pixels.common.physical.PhysicalWriter;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.executor.join.Joiner;
import io.pixelsdb.pixels.planner.plan.physical.domain.PartitionInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.PartitionedTableInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.ShuffleInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.ShuffleQueueInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.StorageInfo;
import io.pixelsdb.pixels.storage.s3qs.S3QS;
import io.pixelsdb.pixels.storage.s3qs.S3QueueMessage;
import io.pixelsdb.pixels.storage.s3qs.S3QueuePollResult;
import org.junit.Test;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assume.assumeTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName.APPROXIMATE_RECEIVE_COUNT;

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

    @Test
    public void s3qsProducerMessagesDrainThroughConsumerProtocol() throws Exception
    {
        SqsClient sqs = mock(SqsClient.class);
        S3QS s3qs = newS3QS(sqs);
        try
        {
            s3qs.registerQueue("shuffle-1", 0, "ignored-name", "queue-url-0");
            setWorkerCommonS3QS(s3qs);

            TypeDescription schema = TypeDescription.createStruct()
                    .addField("key", TypeDescription.createLong());
            ShuffleInfo shuffleInfo = new ShuffleInfo("shuffle-1",
                    new StorageInfo(Storage.Scheme.s3qs, null, null, null, null),
                    "s3://bucket/shuffle-1/", 1, 1, 1, 1, Collections.emptyList());
            S3QueueMessage data = BasePartitionWorker.createS3QSDataMessage(
                    shuffleInfo, 0, 7, 0, 0L, schema);
            S3QueueMessage end = BasePartitionWorker.createS3QSProducerEndMessage(
                    shuffleInfo, 0, 7, 0, 1L, schema);

            when(sqs.receiveMessage(any(ReceiveMessageRequest.class)))
                    .thenReturn(receive("receipt-data", data))
                    .thenReturn(receive("receipt-end", end))
                    .thenReturn(ReceiveMessageResponse.builder().build());

            AtomicInteger dataMessages = new AtomicInteger();
            BasePartitionedJoinWorker.drainS3QSPartition(shuffleInfo, 0,
                    new HashMap<Integer, Queue<S3QueuePollResult>>(),
                    message -> {
                        assertTrue(message.isData());
                        assertEquals(7, message.getProducerId());
                        assertEquals(schema.toString(), message.getMetadata());
                        dataMessages.incrementAndGet();
                    });

            assertEquals(1, dataMessages.get());
            verify(sqs).deleteMessage(DeleteMessageRequest.builder()
                    .queueUrl("queue-url-0").receiptHandle("receipt-data").build());
            verify(sqs).deleteMessage(DeleteMessageRequest.builder()
                    .queueUrl("queue-url-0").receiptHandle("receipt-end").build());
        }
        finally
        {
            setWorkerCommonS3QS(null);
        }
    }

    @Test
    public void awsWorkerCommonShuffleInfoWritesAndDrainsS3QS() throws Exception
    {
        String bucket = System.getenv("PIXELS_S3QS_IT_BUCKET");
        String prefix = trimSlashes(System.getenv("PIXELS_S3QS_IT_PREFIX"));
        String queuePrefix = System.getenv("PIXELS_S3QS_IT_QUEUE_PREFIX");
        assumeTrue("set PIXELS_S3QS_IT_BUCKET to run the AWS worker-common S3QS integration test",
                bucket != null && !bucket.trim().isEmpty());
        assumeTrue("set PIXELS_S3QS_IT_QUEUE_PREFIX to run the AWS worker-common S3QS integration test",
                queuePrefix != null && !queuePrefix.trim().isEmpty());

        String suffix = UUID.randomUUID().toString().replace("-", "");
        String shuffleId = "worker-it-" + suffix;
        String objectRoot = bucket + "/" + (prefix.isEmpty() ? "" : prefix + "/") + shuffleId + "/";
        ShuffleQueueInfo queueInfo = new ShuffleQueueInfo(0, queuePrefix + "-" + suffix, null);
        ShuffleInfo shuffleInfo = new ShuffleInfo(shuffleId,
                new StorageInfo(Storage.Scheme.s3qs, null, null, null, null),
                objectRoot, 1, 1, 1, 1, Collections.singletonList(queueInfo));

        setWorkerCommonS3QS(null);
        WorkerCommon.initOptionalShuffleStorage(shuffleInfo);
        S3QS s3qs = (S3QS) WorkerCommon.getStorage(Storage.Scheme.s3qs);
        byte[] payload = ("worker-payload-" + suffix).getBytes(StandardCharsets.UTF_8);
        try
        {
            TypeDescription schema = TypeDescription.createStruct()
                    .addField("key", TypeDescription.createLong());
            S3QueueMessage data = BasePartitionWorker.createS3QSDataMessage(
                    shuffleInfo, 0, 0, 0, 0L, schema);
            PhysicalWriter writer = s3qs.offer(data);
            writer.append(payload);
            writer.close();
            s3qs.publish(BasePartitionWorker.createS3QSProducerEndMessage(
                    shuffleInfo, 0, 0, 0, 1L, schema));

            AtomicInteger dataMessages = new AtomicInteger();
            BasePartitionedJoinWorker.drainS3QSPartition(shuffleInfo, 0,
                    new HashMap<Integer, Queue<S3QueuePollResult>>(), message -> {
                        PhysicalReader reader = PhysicalReaderUtil.newPhysicalReader(s3qs, message.getObjectPath());
                        ByteBuffer buffer = reader.readFully(payload.length);
                        byte[] actual = new byte[payload.length];
                        buffer.get(actual);
                        reader.close();
                        assertArrayEquals(payload, actual);
                        assertEquals(schema.toString(), message.getMetadata());
                        dataMessages.incrementAndGet();
                    });

            assertEquals(1, dataMessages.get());
        }
        finally
        {
            if (queueInfo.getQueueUrl() != null)
            {
                s3qs.deleteQueue(queueInfo.getQueueUrl());
            }
            s3qs.delete(objectRoot, true);
            setWorkerCommonS3QS(null);
        }
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

    private static S3QS newS3QS(SqsClient sqs) throws Exception
    {
        S3QS s3qs = new S3QS(30);
        Field sqsField = S3QS.class.getDeclaredField("sqs");
        sqsField.setAccessible(true);
        sqsField.set(s3qs, sqs);
        return s3qs;
    }

    private static void setWorkerCommonS3QS(S3QS s3qs) throws Exception
    {
        Field s3qsField = WorkerCommon.class.getDeclaredField("s3qs");
        s3qsField.setAccessible(true);
        s3qsField.set(null, s3qs);
    }

    private static ReceiveMessageResponse receive(String receiptHandle, S3QueueMessage body) throws Exception
    {
        Message message = Message.builder()
                .body(body.toMessageBody())
                .receiptHandle(receiptHandle)
                .attributes(receiveCount(1))
                .build();
        return ReceiveMessageResponse.builder().messages(message).build();
    }

    private static Map<MessageSystemAttributeName, String> receiveCount(int count)
    {
        return Collections.singletonMap(APPROXIMATE_RECEIVE_COUNT, String.valueOf(count));
    }

    private static String trimSlashes(String value)
    {
        if (value == null)
        {
            return "";
        }
        String result = value.trim();
        while (result.startsWith("/"))
        {
            result = result.substring(1);
        }
        while (result.endsWith("/"))
        {
            result = result.substring(0, result.length() - 1);
        }
        return result;
    }
}
