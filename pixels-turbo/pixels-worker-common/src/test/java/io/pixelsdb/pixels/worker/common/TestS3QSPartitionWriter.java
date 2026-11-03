/* Copyright 2026 PixelsDB. Licensed under the AGPL-3.0 license. */
package io.pixelsdb.pixels.worker.common;

import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.FixSizedBuffers;
import io.pixelsdb.pixels.common.utils.Constants;
import io.pixelsdb.pixels.core.*;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.vector.*;
import io.pixelsdb.pixels.planner.plan.physical.domain.ShuffleInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.StorageInfo;
import io.pixelsdb.pixels.storage.s3qs.*;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class TestS3QSPartitionWriter
{
    @Test(timeout = 15000)
    public void columnThreadsAreReusedAcrossFilesWithoutChangingEncodedBytes() throws Exception
    {
        Fixture baseline = new Fixture(longSchema()), reused = new Fixture(longSchema());
        java.util.concurrent.atomic.AtomicInteger starts = new java.util.concurrent.atomic.AtomicInteger();
        java.util.concurrent.ExecutorService pool = java.util.concurrent.Executors.newCachedThreadPool(task -> {
            starts.incrementAndGet(); return new Thread(task, "test-reused-columns");
        });
        try
        {
            S3QSPartitionWriter oldWriter = new S3QSPartitionWriter(baseline.storage, baseline.schema, baseline.shuffle,
                    0, 0, baseline.context, 8192);
            S3QSPartitionWriter newWriter = new S3QSPartitionWriter(reused.storage, reused.schema, reused.shuffle,
                    0, 0, reused.context, 8192, new java.util.concurrent.atomic.AtomicLong(), () -> {},
                    new FixSizedBuffers(8192, 1), pool);
            for (int i = 0; i < 32; i++)
            {
                List<VectorizedRowBatch> batches = Collections.singletonList(longs(128, i * 128));
                oldWriter.write(batches); newWriter.write(batches);
                assertFalse("file close must not shut down the shared executor", pool.isShutdown());
            }
            assertEquals(32, reused.objects.size());
            assertTrue("reuse must create fewer threads than one pool per file", starts.get() < 32);
            Iterator<byte[]> expected = baseline.objects.values().iterator();
            for (byte[] bytes : reused.objects.values()) { assertArrayEquals(expected.next(), bytes); }
            assertEquals(4096, reused.verifyLongs());
        }
        finally
        {
            pool.shutdownNow(); assertTrue(pool.awaitTermination(1, java.util.concurrent.TimeUnit.SECONDS));
        }
    }

    @Test
    public void scratchIsReusedAcrossOversizedTrialsAndLaterFilesWithoutChangingValues() throws Exception
    {
        Fixture fixture = new Fixture(longSchema());
        FixSizedBuffers buffers = new FixSizedBuffers(8192, 1);
        byte[] first = buffers.allocate(); buffers.free(first);
        S3QSPartitionWriter writer = new S3QSPartitionWriter(fixture.storage, fixture.schema, fixture.shuffle,
                0, 0, fixture.context, 8192, new java.util.concurrent.atomic.AtomicLong(), () -> {}, buffers);
        writer.write(Collections.singletonList(longs(2048, 0)));
        writer.write(Collections.singletonList(longs(128, 2048)));
        assertTrue(fixture.objects.size() > 2);
        assertEquals(2176, fixture.verifyLongs());
        assertEquals(1, buffers.getCachedBufferCount());
        assertSame(first, buffers.allocate());
    }

    @Test(timeout = 10000)
    public void scratchReturnsAfterEncodingAndUploadFailures() throws Exception
    {
        Fixture fixture = new Fixture(longSchema());
        FixSizedBuffers buffers = new FixSizedBuffers(8192, 1);
        S3QSPartitionWriter writer = new S3QSPartitionWriter(fixture.storage, fixture.schema, fixture.shuffle,
                0, 0, fixture.context, 8192, new java.util.concurrent.atomic.AtomicLong(), () -> {}, buffers);
        VectorizedRowBatch invalid = longs(1, 0);
        invalid.cols[0] = new BinaryColumnVector(1);
        assertThrows(java.util.concurrent.CompletionException.class, () -> writer.write(Collections.singletonList(invalid)));
        assertEquals(1, buffers.getCachedBufferCount()); assertTrue(fixture.objects.isEmpty());
        when(fixture.s3.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenThrow(software.amazon.awssdk.core.exception.SdkClientException.create("injected PUT failure"));
        assertThrows(software.amazon.awssdk.core.exception.SdkClientException.class,
                () -> writer.write(Collections.singletonList(longs(1, 0))));
        assertEquals(1, buffers.getCachedBufferCount()); assertTrue(fixture.objects.isEmpty());
        assertTrue(fixture.messages.isEmpty());
    }

    @Test
    public void c3SizedPartitionSplitsIntoReadableObjectsAndCompletes() throws Exception
    {
        checkLargePartition(524288);
    }

    @Test
    public void c6SizedHotPartitionSplitsIntoReadableObjectsAndCompletes() throws Exception
    {
        checkLargePartition(1048576);
    }

    private static void checkLargePartition(int rows) throws Exception
    {
        Fixture fixture = new Fixture(longSchema());
        VectorizedRowBatch input = longs(rows, 0);
        fixture.produce(Collections.singletonList(input));
        assertTrue(fixture.objects.size() > 1);
        assertEquals(rows, fixture.verifyLongs());
        fixture.verifyEndAndDrain();
        assertEquals(fixture.objects.values().stream().mapToLong(bytes -> bytes.length).sum(),
                fixture.context.getWorkerMetrics().getWriteBytes());
    }

    @Test
    public void multipleBatchesAndEmptyBatchesPreserveOrder() throws Exception
    {
        Fixture fixture = new Fixture(longSchema());
        List<VectorizedRowBatch> batches = Arrays.asList(longs(128, 0), longs(0, 0),
                longs(128, 128), longs(128, 256));
        long count = fixture.write(batches, 8192);
        assertTrue(count > 1);
        assertEquals(384, fixture.verifyLongs());
        assertEquals(count, fixture.messages.size());
    }

    @Test
    public void variableWidthAndNullRowsSplitWithinBatch() throws Exception
    {
        TypeDescription schema = TypeDescription.createStruct().addField("k", TypeDescription.createLong())
                .addField("v", TypeDescription.createVarchar(1024));
        VectorizedRowBatch input = schema.createRowBatch(17);
        for (int row = 0; row < 17; ++row)
        {
            ((LongColumnVector) input.cols[0]).vector[row] = row;
            BinaryColumnVector values = (BinaryColumnVector) input.cols[1];
            if (row % 5 == 0)
            {
                values.noNulls = false;
                values.isNull[row] = true;
            }
            else
            {
                byte[] value = new byte[600 + row];
                new Random(row).nextBytes(value);
                for (int i = 0; i < value.length; ++i) { value[i] = (byte) ('a' + (value[i] & 0xff) % 26); }
                values.setRef(row, value, 0, value.length);
            }
        }
        input.size = 17;
        Fixture fixture = new Fixture(schema);
        assertTrue(fixture.write(Collections.singletonList(input), 8192) > 1);
        int seen = 0;
        for (S3QueueMessage message : fixture.messages)
        {
            try (PixelsReader reader = fixture.reader(message);
                 PixelsRecordReader records = reader.read(new PixelsReaderOption().includeCols(new String[]{"k", "v"})))
            {
                while (true)
                {
                    VectorizedRowBatch batch = records.readBatch(100);
                    for (int row = 0; row < batch.size; ++row, ++seen)
                    {
                        assertEquals(seen, ((LongColumnVector) batch.cols[0]).vector[row]);
                        BinaryColumnVector values = (BinaryColumnVector) batch.cols[1];
                        assertEquals(seen % 5 == 0, values.isNull[row]);
                        if (seen % 5 != 0)
                        {
                            BinaryColumnVector original = (BinaryColumnVector) input.cols[1];
                            assertArrayEquals(original.vector[seen], Arrays.copyOfRange(values.vector[row],
                                    values.start[row], values.start[row] + values.lens[row]));
                        }
                    }
                    if (batch.endOfFile) { break; }
                }
            }
        }
        assertEquals(17, seen);
    }

    @Test
    public void completeFileAtLimitFitsButOneByteLessRejectsSingleRow() throws Exception
    {
        VectorizedRowBatch input = longs(1, 0);
        Fixture baseline = new Fixture(longSchema());
        baseline.write(Collections.singletonList(input), Constants.S3QS_BUFFER_SIZE);
        int fileSize = baseline.objects.values().iterator().next().length;
        Fixture exact = new Fixture(longSchema());
        assertEquals(1, exact.write(Collections.singletonList(input), fileSize));
        assertEquals(1, exact.verifyLongs());
        Fixture below = new Fixture(longSchema());
        IOException failure = assertThrows(IOException.class, () ->
                below.write(Collections.singletonList(input), fileSize - 1));
        assertTrue(failure.getMessage().contains("single row"));
        assertTrue(below.objects.isEmpty());
        assertTrue(below.messages.isEmpty());
    }

    @Test
    public void oversizedSingleRowDoesNotUploadOrPublish() throws Exception
    {
        TypeDescription schema = TypeDescription.createStruct()
                .addField("v", TypeDescription.createVarbinary(Constants.S3QS_BUFFER_SIZE + 1));
        VectorizedRowBatch input = schema.createRowBatch(1);
        byte[] value = new byte[Constants.S3QS_BUFFER_SIZE + 1];
        new Random(1277).nextBytes(value);
        ((BinaryColumnVector) input.cols[0]).setRef(0, value, 0, value.length);
        input.size = 1;
        Fixture fixture = new Fixture(schema);
        assertThrows(IOException.class, () -> fixture.produce(Collections.singletonList(input)));
        assertTrue(fixture.objects.isEmpty());
        assertTrue(fixture.messages.isEmpty());
    }

    @Test
    public void laterUploadFailureDoesNotPublishNormalEnd() throws Exception
    {
        Fixture fixture = new Fixture(longSchema());
        final int[] uploads = {0};
        doAnswer(call -> {
            if (++uploads[0] == 2) { throw S3Exception.builder().message("injected upload failure").build(); }
            return fixture.store(call.getArgument(0), call.getArgument(1));
        })
                .when(fixture.s3).putObject(any(PutObjectRequest.class), any(RequestBody.class));
        assertThrows(S3Exception.class, () -> fixture.produce(Collections.singletonList(longs(524288, 0))));
        assertEquals(2, uploads[0]);
        assertEquals(1, fixture.objects.size());
        assertEquals(1, fixture.messages.size());
        assertTrue(fixture.messages.get(0).isData());
    }

    @Test
    public void cancellationAfterUploadPreventsDataAndEnd() throws Exception
    {
        Fixture fixture = new Fixture(longSchema());
        doAnswer(call -> {
            PutObjectResponse response = fixture.store(call.getArgument(0), call.getArgument(1));
            fixture.context.getCancellation().cancel(new IOException("cancelled after upload"));
            return response;
        }).when(fixture.s3).putObject(any(PutObjectRequest.class), any(RequestBody.class));
        assertThrows(WorkerException.class, () -> fixture.produce(Collections.singletonList(longs(1, 0))));
        assertEquals(1, fixture.objects.size());
        assertTrue(fixture.messages.isEmpty());
    }

    @Test
    public void emptyPartitionHasZeroEndAndNoData() throws Exception
    {
        Fixture fixture = new Fixture(longSchema());
        fixture.produce(Collections.singletonList(longs(0, 0)));
        assertTrue(fixture.objects.isEmpty());
        fixture.verifyEndAndDrain();
        assertEquals(0, fixture.messages.get(0).getSequenceId());
    }

    @Test
    public void cancelledEncodingDoesNotUploadOrPublish() throws Exception
    {
        Fixture fixture = new Fixture(longSchema());
        fixture.context.getCancellation().cancel(new IOException("cancelled"));
        assertThrows(WorkerException.class, () -> fixture.produce(Collections.singletonList(longs(1, 0))));
        assertTrue(fixture.objects.isEmpty());
        assertTrue(fixture.messages.isEmpty());
    }

    private static TypeDescription longSchema()
    {
        TypeDescription schema = TypeDescription.createStruct();
        for (String name : new String[]{"k", "id", "p0", "p1"}) { schema.addField(name, TypeDescription.createLong()); }
        return schema;
    }

    private static long mix(long value)
    {
        value += 0x9e3779b97f4a7c15L;
        value = (value ^ value >>> 30) * 0xbf58476d1ce4e5b9L;
        value = (value ^ value >>> 27) * 0x94d049bb133111ebL;
        return value ^ value >>> 31;
    }

    private static VectorizedRowBatch longs(int rows, int start)
    {
        VectorizedRowBatch batch = longSchema().createRowBatch(Math.max(1, rows));
        for (int row = 0; row < rows; ++row)
        {
            for (int col = 0; col < 4; ++col)
            {
                long id = start + row;
                ((LongColumnVector) batch.cols[col]).vector[row] = col < 2 ? id : mix(id + 1277 + col);
            }
        }
        batch.size = rows;
        return batch;
    }

    private static final class Fixture
    {
        final TypeDescription schema;
        final S3Client s3 = mock(S3Client.class);
        final SqsClient sqs = mock(SqsClient.class);
        final S3QS storage = new S3QS(s3, sqs, 30);
        final Map<String, byte[]> objects = new LinkedHashMap<>();
        final List<S3QueueMessage> messages = new ArrayList<>();
        final WorkerContext context = new WorkerContext(mock(Logger.class), new WorkerMetrics(), "bounded-test")
                .withCancellation(new WorkerCancellation());
        final ShuffleInfo shuffle = new ShuffleInfo("bounded-test",
                new StorageInfo(Storage.Scheme.s3qs, null, null, null, null),
                "bucket/shuffle/", 1, 1, 1, 1, Collections.emptyList());

        Fixture(TypeDescription schema) throws Exception
        {
            this.schema = schema;
            storage.registerQueue(shuffle.getShuffleId(), 0, "ignored", "queue-url");
            when(s3.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                    .thenAnswer(call -> store(call.getArgument(0), call.getArgument(1)));
            when(sqs.sendMessage(any(SendMessageRequest.class))).thenAnswer(call ->
            {
                S3QueueMessage message = S3QueueMessage.fromMessageBody(((SendMessageRequest) call.getArgument(0)).messageBody());
                if (message.isData())
                {
                    assertTrue(objects.containsKey(message.getObjectPath()));
                    assertEquals(objects.get(message.getObjectPath()).length, message.getByteSize());
                }
                messages.add(message);
                return SendMessageResponse.builder().messageId("local").build();
            });
            when(s3.getObject(any(GetObjectRequest.class),
                    org.mockito.ArgumentMatchers.<ResponseTransformer<GetObjectResponse, ResponseBytes<GetObjectResponse>>>any()))
                    .thenAnswer(call -> {
                        GetObjectRequest request = call.getArgument(0);
                        return ResponseBytes.fromByteArray(GetObjectResponse.builder().build(),
                                objects.get(request.bucket() + "/" + request.key()));
                    });
        }

        PutObjectResponse store(PutObjectRequest request, RequestBody body) throws IOException
        {
            byte[] bytes = new byte[(int) body.contentLength()];
            try (java.io.DataInputStream input = new java.io.DataInputStream(body.contentStreamProvider().newStream()))
            {
                input.readFully(bytes);
            }
            assertTrue(bytes.length <= Constants.S3QS_BUFFER_SIZE);
            objects.put(request.bucket() + "/" + request.key(), bytes);
            return PutObjectResponse.builder().build();
        }

        long write(List<VectorizedRowBatch> batches, int limit) throws IOException
        {
            long count = new S3QSPartitionWriter(storage, schema, shuffle, 0, 0, context, limit).write(batches);
            assertEquals(count, objects.size());
            for (byte[] bytes : objects.values()) { assertTrue(bytes.length <= limit); }
            return count;
        }

        void produce(List<VectorizedRowBatch> batches) throws Exception
        {
            try (S3QSAsyncPartitionOutput output = new S3QSAsyncPartitionOutput(storage, shuffle, 0, 1, context))
            {
                for (VectorizedRowBatch batch : batches) { output.add(0, batch, schema); }
                output.finish(schema, new HashSet<>());
            }
        }

        PixelsReader reader(S3QueueMessage message) throws IOException
        {
            return PixelsReaderImpl.newBuilder().setStorage(storage).setPath(message.getObjectPath())
                    .setPixelsFooterCache(new PixelsFooterCache()).build();
        }

        int verifyLongs() throws Exception
        {
            int seen = 0;
            for (S3QueueMessage message : messages)
            {
                if (!message.isData()) { continue; }
                long objectRows = 0;
                try (PixelsReader reader = reader(message);
                     PixelsRecordReader records = reader.read(new PixelsReaderOption().includeCols(new String[]{"k", "id", "p0", "p1"})))
                {
                    while (true)
                    {
                        VectorizedRowBatch batch = records.readBatch(4096);
                        for (int row = 0; row < batch.size; ++row, ++seen, ++objectRows)
                        {
                            for (int col = 0; col < 4; ++col)
                            {
                                assertEquals(col < 2 ? seen : mix(seen + 1277L + col),
                                        ((LongColumnVector) batch.cols[col]).vector[row]);
                            }
                        }
                        if (batch.endOfFile) { break; }
                    }
                }
                assertEquals(objectRows, message.getRowCount());
            }
            return seen;
        }

        void verifyEndAndDrain() throws Exception
        {
            for (int i = 0; i < messages.size(); ++i) { assertEquals(i, messages.get(i).getSequenceId()); }
            assertTrue(messages.get(messages.size() - 1).isProducerEnd());
            assertEquals(objects.size(), messages.get(messages.size() - 1).getSequenceId());
            Field cached = WorkerCommon.class.getDeclaredField("s3qs");
            cached.setAccessible(true);
            Object previous = cached.get(null);
            cached.set(null, storage);
            try
            {
                Queue<S3QueuePollResult> pending = new ConcurrentLinkedQueue<>();
                for (int i = 0; i < messages.size(); ++i) { pending.add(new S3QueuePollResult("receipt-" + i, messages.get(i))); }
                when(sqs.receiveMessage(any(software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest.class)))
                        .thenReturn(software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse.builder().build());
                Map<Integer, Queue<S3QueuePollResult>> queues = new HashMap<>();
                queues.put(0, pending);
                final int[] handled = {0};
                BasePartitionedJoinWorker.drainS3QSPartition(shuffle, 0, queues, message -> handled[0]++);
                assertEquals(objects.size(), handled[0]);
            }
            finally { cached.set(null, previous); }
        }
    }
}
