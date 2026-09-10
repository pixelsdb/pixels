/*
 * Copyright 2025 PixelsDB.
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
package io.pixelsdb.pixels.storage.s3qs;

import io.pixelsdb.pixels.common.physical.PhysicalReader;
import io.pixelsdb.pixels.common.physical.PhysicalReaderUtil;
import io.pixelsdb.pixels.common.physical.PhysicalWriter;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName.APPROXIMATE_RECEIVE_COUNT;

/**
 * Unit tests for the current S3QS shuffle queue protocol.
 */
public class TestS3QS
{
    private static final String QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/123456789012/pixels-shuffle-test";

    @Test
    public void testDataMessageRoundTrip() throws IOException
    {
        S3QueueMessage message = S3QueueMessage.data("shuffle-1", 3, 7, 2, 99L,
                        "bucket/shuffle/3/object")
                .setTimestamp(12345L)
                .setRowCount(1000L)
                .setByteSize(4096L)
                .setMetadata("source=unit");

        S3QueueMessage parsed = S3QueueMessage.fromMessageBody(message.toMessageBody());

        assertTrue(parsed.isData());
        assertFalse(parsed.isProducerEnd());
        assertEquals("shuffle-1", parsed.getShuffleId());
        assertEquals(3, parsed.getPartitionId());
        assertEquals(3, parsed.getPartitionNum());
        assertEquals(7, parsed.getProducerId());
        assertEquals(7, parsed.getWorkerNum());
        assertEquals(2, parsed.getProducerAttemptId());
        assertEquals(99L, parsed.getSequenceId());
        assertEquals(12345L, parsed.getTimestamp());
        assertEquals("bucket/shuffle/3/object", parsed.getObjectPath());
        assertEquals(1000L, parsed.getRowCount());
        assertEquals(4096L, parsed.getByteSize());
        assertEquals("source=unit", parsed.getMetadata());
    }

    @Test
    public void testProducerEndMessageRoundTrip() throws IOException
    {
        S3QueueMessage message = S3QueueMessage.producerEnd("shuffle-2", 5, 11, 4, 123L)
                .setTimestamp(67890L)
                .setMetadata("marker=true");

        S3QueueMessage parsed = S3QueueMessage.fromMessageBody(message.toMessageBody());

        assertTrue(parsed.isProducerEnd());
        assertFalse(parsed.isData());
        assertEquals(S3QueueMessage.MessageType.PRODUCER_END, parsed.getMessageType());
        assertEquals("shuffle-2", parsed.getShuffleId());
        assertEquals(5, parsed.getPartitionId());
        assertEquals(11, parsed.getProducerId());
        assertEquals(4, parsed.getProducerAttemptId());
        assertEquals(123L, parsed.getSequenceId());
        assertEquals(67890L, parsed.getTimestamp());
        assertEquals("NORMAL", parsed.getEndReason());
        assertEquals("marker=true", parsed.getMetadata());
        assertTrue(parsed.getEndWork());
    }

    @Test
    public void testRegisterQueueWithResolvedUrlIsLocalAndIdempotent() throws Exception
    {
        SqsClient sqs = mock(SqsClient.class);
        S3QS s3qs = newS3QS(sqs);

        String firstUrl = s3qs.registerQueue("shuffle-idempotent", 3, "ignored-name", QUEUE_URL);
        String secondUrl = s3qs.registerQueue("shuffle-idempotent", 3, "another-name", QUEUE_URL);

        assertEquals(QUEUE_URL, firstUrl);
        assertEquals(QUEUE_URL, secondUrl);
        verify(sqs, never()).createQueue(any(CreateQueueRequest.class));
    }

    @Test(expected = IOException.class)
    public void testRegisterQueueRejectsConflictingUrlForSameShufflePartition() throws Exception
    {
        S3QS s3qs = newS3QS(mock(SqsClient.class));
        s3qs.registerQueue("shuffle-conflict", 3, "ignored-name", QUEUE_URL);
        s3qs.registerQueue("shuffle-conflict", 3, "another-name", "another-url");
    }

    @Test(expected = IOException.class)
    public void testRegisterQueueRejectsEmptyShuffleId() throws Exception
    {
        S3QS s3qs = newS3QS(mock(SqsClient.class));
        s3qs.registerQueue(" ", 0, "ignored-name", QUEUE_URL);
    }

    @Test
    public void testConcurrentRegistrationCreatesOneQueueForOneShufflePartition() throws Exception
    {
        SqsClient sqs = mock(SqsClient.class);
        when(sqs.getQueueUrl(any(GetQueueUrlRequest.class)))
                .thenReturn(GetQueueUrlResponse.builder().queueUrl(QUEUE_URL).build());
        S3QS s3qs = newS3QS(sqs);
        ExecutorService executor = Executors.newFixedThreadPool(8);
        try
        {
            Callable<String> registration =
                    () -> s3qs.registerQueue("shuffle-concurrent", 0, "concurrent-queue", null);
            List<Future<String>> results = executor.invokeAll(Collections.nCopies(16, registration));

            for (Future<String> result : results)
            {
                assertEquals(QUEUE_URL, result.get());
            }
            verify(sqs, times(1)).createQueue(any(CreateQueueRequest.class));
        }
        finally
        {
            executor.shutdownNow();
        }
    }

    @Test
    public void testSamePartitionInDifferentShufflesRoutesIndependently() throws Exception
    {
        String smallQueueUrl = QUEUE_URL + "-small";
        String largeQueueUrl = QUEUE_URL + "-large";
        SqsClient sqs = mock(SqsClient.class);
        S3QS s3qs = newS3QS(sqs);
        s3qs.registerQueue("small-shuffle", 0, "small", smallQueueUrl);
        s3qs.registerQueue("large-shuffle", 0, "large", largeQueueUrl);

        S3QueueMessage small = S3QueueMessage.producerEnd("small-shuffle", 0, 1, 0, 1L);
        S3QueueMessage large = S3QueueMessage.producerEnd("large-shuffle", 0, 2, 0, 1L);
        s3qs.publish(small);
        s3qs.publish(large);

        ArgumentCaptor<SendMessageRequest> sends = ArgumentCaptor.forClass(SendMessageRequest.class);
        verify(sqs, times(2)).sendMessage(sends.capture());
        assertEquals(smallQueueUrl, sends.getAllValues().get(0).queueUrl());
        assertEquals(largeQueueUrl, sends.getAllValues().get(1).queueUrl());

        when(sqs.receiveMessage(any(ReceiveMessageRequest.class))).thenAnswer(invocation ->
        {
            ReceiveMessageRequest request = invocation.getArgument(0);
            if (smallQueueUrl.equals(request.queueUrl()))
            {
                return receive("small-receipt", small);
            }
            if (largeQueueUrl.equals(request.queueUrl()))
            {
                return receive("large-receipt", large);
            }
            throw new AssertionError("unexpected queue URL: " + request.queueUrl());
        });

        S3QueuePollResult smallResult = s3qs.pollMessage("small-shuffle", 0, 1);
        S3QueuePollResult largeResult = s3qs.pollMessage("large-shuffle", 0, 1);
        assertEquals("small-shuffle", smallResult.getMessage().getShuffleId());
        assertEquals("large-shuffle", largeResult.getMessage().getShuffleId());

        smallResult.getMessage().setReceiptHandle(smallResult.getReceiptHandle());
        largeResult.getMessage().setReceiptHandle(largeResult.getReceiptHandle());
        assertEquals(0, s3qs.finishWork(smallResult.getMessage()));
        assertEquals(0, s3qs.finishWork(largeResult.getMessage()));
        verify(sqs).deleteMessage(DeleteMessageRequest.builder()
                .queueUrl(smallQueueUrl).receiptHandle("small-receipt").build());
        verify(sqs).deleteMessage(DeleteMessageRequest.builder()
                .queueUrl(largeQueueUrl).receiptHandle("large-receipt").build());
    }

    @Test
    public void testPublishSendsStructuredProducerEndMessage() throws Exception
    {
        SqsClient sqs = mock(SqsClient.class);
        S3QS s3qs = newS3QS(sqs);
        s3qs.registerQueue("shuffle-3", 9, "ignored-name", QUEUE_URL);

        S3QueueMessage marker = S3QueueMessage.producerEnd("shuffle-3", 9, 17, 1, 88L);
        s3qs.publish(marker);

        ArgumentCaptor<SendMessageRequest> requestCaptor = ArgumentCaptor.forClass(SendMessageRequest.class);
        verify(sqs).sendMessage(requestCaptor.capture());
        SendMessageRequest request = requestCaptor.getValue();
        S3QueueMessage sent = S3QueueMessage.fromMessageBody(request.messageBody());

        assertEquals(QUEUE_URL, request.queueUrl());
        assertTrue(sent.isProducerEnd());
        assertEquals("shuffle-3", sent.getShuffleId());
        assertEquals(9, sent.getPartitionId());
        assertEquals(17, sent.getProducerId());
        assertEquals(1, sent.getProducerAttemptId());
        assertEquals(88L, sent.getSequenceId());
    }

    @Test
    public void testPollMessageParsesStructuredMessageAndReceiptHandle() throws Exception
    {
        SqsClient sqs = mock(SqsClient.class);
        S3QS s3qs = newS3QS(sqs);
        s3qs.registerQueue("shuffle-4", 4, "ignored-name", QUEUE_URL);
        S3QueueMessage body = S3QueueMessage.data("shuffle-4", 4, 2, 0, 10L,
                "bucket/shuffle/4/object");

        Message sqsMessage = Message.builder()
                .body(body.toMessageBody())
                .receiptHandle("receipt-1")
                .attributes(receiveCount(1))
                .build();
        when(sqs.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenReturn(ReceiveMessageResponse.builder().messages(sqsMessage).build());

        S3QueuePollResult result = s3qs.pollMessage("shuffle-4", 4, 99);

        assertNotNull(result);
        assertEquals("receipt-1", result.getReceiptHandle());
        assertTrue(result.getMessage().isData());
        assertEquals("shuffle-4", result.getMessage().getShuffleId());
        assertEquals("bucket/shuffle/4/object", result.getMessage().getObjectPath());

        ArgumentCaptor<ReceiveMessageRequest> requestCaptor = ArgumentCaptor.forClass(ReceiveMessageRequest.class);
        verify(sqs).receiveMessage(requestCaptor.capture());
        assertEquals(QUEUE_URL, requestCaptor.getValue().queueUrl());
        assertEquals(Integer.valueOf(20), requestCaptor.getValue().waitTimeSeconds());
    }

    @Test(expected = IOException.class)
    public void testCompatibilityPollRejectsControlMessage() throws Exception
    {
        SqsClient sqs = mock(SqsClient.class);
        S3QS s3qs = newS3QS(sqs);
        s3qs.registerQueue("shuffle-5", 6, "ignored-name", QUEUE_URL);
        S3QueueMessage body = S3QueueMessage.producerEnd("shuffle-5", 6, 2, 0, 10L);

        Message sqsMessage = Message.builder()
                .body(body.toMessageBody())
                .receiptHandle("receipt-control")
                .attributes(receiveCount(1))
                .build();
        when(sqs.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenReturn(ReceiveMessageResponse.builder().messages(sqsMessage).build());

        s3qs.poll(new S3QueueMessage().setShuffleId("shuffle-5").setPartitionId(6), 1);
    }

    @Test
    public void testFinishWorkDeletesReceiptHandle() throws Exception
    {
        SqsClient sqs = mock(SqsClient.class);
        S3QS s3qs = newS3QS(sqs);
        s3qs.registerQueue("shuffle-ack", 8, "ignored-name", QUEUE_URL);

        int result = s3qs.finishWork(new S3QueueMessage()
                .setShuffleId("shuffle-ack")
                .setPartitionId(8)
                .setReceiptHandle("receipt-finished"));

        assertEquals(0, result);
        ArgumentCaptor<DeleteMessageRequest> requestCaptor = ArgumentCaptor.forClass(DeleteMessageRequest.class);
        verify(sqs).deleteMessage(requestCaptor.capture());
        assertEquals(QUEUE_URL, requestCaptor.getValue().queueUrl());
        assertEquals("receipt-finished", requestCaptor.getValue().receiptHandle());
    }

    @Test(expected = IOException.class)
    public void testUnregisteredPartitionCannotPublish() throws Exception
    {
        S3QS s3qs = newS3QS(mock(SqsClient.class));
        s3qs.publish(S3QueueMessage.producerEnd("shuffle-6", 12, 1, 0, 1L));
    }

    @Test
    public void testAwsS3QSOfferPollReadAckAndCleanupIntegration() throws Exception
    {
        String bucket = System.getenv("PIXELS_S3QS_IT_BUCKET");
        String prefix = trimSlashes(System.getenv("PIXELS_S3QS_IT_PREFIX"));
        String queuePrefix = System.getenv("PIXELS_S3QS_IT_QUEUE_PREFIX");
        assumeTrue("set PIXELS_S3QS_IT_BUCKET to run the AWS S3QS integration test",
                bucket != null && !bucket.trim().isEmpty());
        assumeTrue("set PIXELS_S3QS_IT_QUEUE_PREFIX to run the AWS S3QS integration test",
                queuePrefix != null && !queuePrefix.trim().isEmpty());

        String suffix = UUID.randomUUID().toString().replace("-", "");
        String shuffleId = "it-" + suffix;
        String queueUrl = null;
        String objectRoot = bucket + "/" + (prefix.isEmpty() ? "" : prefix + "/") + shuffleId + "/";
        S3QS s3qs = new S3QS(30);

        try
        {
            queueUrl = s3qs.registerQueue(shuffleId, 0, queuePrefix + "-" + suffix, null);
            byte[] payload = ("payload-" + suffix).getBytes(StandardCharsets.UTF_8);
            S3QueueMessage dataMessage = S3QueueMessage.data(shuffleId, 0, 3, 0, 0L, objectRoot)
                    .setMetadata("schema=raw-bytes");

            PhysicalWriter writer = s3qs.offer(dataMessage);
            writer.append(payload);
            writer.close();

            S3QueuePollResult dataResult = pollUntilMessage(s3qs, shuffleId, 0, 5);
            assertNotNull(dataResult);
            assertTrue(dataResult.getMessage().isData());
            assertEquals(shuffleId, dataResult.getMessage().getShuffleId());
            assertEquals(0, dataResult.getMessage().getPartitionId());
            assertEquals(3, dataResult.getMessage().getProducerId());
            assertEquals("schema=raw-bytes", dataResult.getMessage().getMetadata());

            PhysicalReader reader = PhysicalReaderUtil.newPhysicalReader(s3qs,
                    dataResult.getMessage().getObjectPath());
            ByteBuffer buffer = reader.readFully(payload.length);
            byte[] actual = new byte[payload.length];
            buffer.get(actual);
            reader.close();
            assertEquals(new String(payload, StandardCharsets.UTF_8),
                    new String(actual, StandardCharsets.UTF_8));

            dataResult.getMessage().setReceiptHandle(dataResult.getReceiptHandle());
            assertEquals(0, s3qs.finishWork(dataResult.getMessage()));

            S3QueueMessage endMessage = S3QueueMessage.producerEnd(shuffleId, 0, 3, 0, 1L);
            s3qs.publish(endMessage);
            S3QueuePollResult endResult = pollUntilMessage(s3qs, shuffleId, 0, 5);
            assertNotNull(endResult);
            assertTrue(endResult.getMessage().isProducerEnd());
            endResult.getMessage().setReceiptHandle(endResult.getReceiptHandle());
            assertEquals(0, s3qs.finishWork(endResult.getMessage()));
        }
        finally
        {
            if (queueUrl != null)
            {
                s3qs.deleteQueue(queueUrl);
            }
            s3qs.delete(objectRoot, true);
        }
    }

    private static S3QS newS3QS(SqsClient sqs) throws Exception
    {
        S3QS s3qs = new S3QS(30);
        Field sqsField = S3QS.class.getDeclaredField("sqs");
        sqsField.setAccessible(true);
        sqsField.set(s3qs, sqs);
        return s3qs;
    }

    private static Map<MessageSystemAttributeName, String> receiveCount(int count)
    {
        return Collections.singletonMap(APPROXIMATE_RECEIVE_COUNT, String.valueOf(count));
    }

    private static S3QueuePollResult pollUntilMessage(S3QS s3qs, String shuffleId,
                                                      int partitionId, int attempts) throws IOException
    {
        for (int i = 0; i < attempts; ++i)
        {
            S3QueuePollResult result = s3qs.pollMessage(shuffleId, partitionId, 1);
            if (result != null)
            {
                return result;
            }
        }
        return null;
    }

    private static ReceiveMessageResponse receive(String receiptHandle, S3QueueMessage message) throws IOException
    {
        Message sqsMessage = Message.builder()
                .body(message.toMessageBody())
                .receiptHandle(receiptHandle)
                .attributes(receiveCount(1))
                .build();
        return ReceiveMessageResponse.builder().messages(sqsMessage).build();
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
