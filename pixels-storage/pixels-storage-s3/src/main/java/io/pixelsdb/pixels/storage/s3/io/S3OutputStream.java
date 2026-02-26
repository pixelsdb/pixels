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
package io.pixelsdb.pixels.storage.s3.io;

import io.pixelsdb.pixels.common.physical.FixSizedBuffers;
import io.pixelsdb.pixels.common.physical.scheduler.RetryPolicy;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.common.utils.ShutdownHookManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static io.pixelsdb.pixels.common.utils.Constants.S3_BUFFER_SIZE;

/**
 * The output stream for AWS S3 compatible storage systems.
 * <p>
 * Referenced the implementation from
 * <a href="https://gist.github.com/blagerweij/ad1dbb7ee2fff8bcffd372815ad310eb">Barry Lagerweij</a>.
 * </p>
 *
 * @author hank
 * @create 2021-09-24
 */
public class S3OutputStream extends OutputStream
{
    private static final Logger logger = LogManager.getLogger(S3OutputStream.class);

    /**
     * The bucket-name on Amazon S3
     */
    private final String bucket;

    /**
     * The path (key) name within the bucket
     */
    private final String key;

    /**
     * The temporary buffer used for storing the chunks
     */
    private byte[] buffer;

    /**
     * The position in the buffer
     */
    private int position;

    /**
     * Amazon S3 client.
     */
    private final S3Client s3Client;

    /**
     * The unique id for this upload
     */
    private String uploadId = null;

    /**
     * Collection of the etags for the parts that have been uploaded
     */
    private final ConcurrentLinkedQueue<CompletableFuture<CompletedPart>> parts;

    /**
     * The number of parts to upload
     */
    private final AtomicInteger numParts = new AtomicInteger(0);

    /**
     * Indicates whether the stream is still open / valid
     */
    private boolean open;

    /**
     * The number of UploadPart requests that are executing
     */
    private final AtomicInteger concurrency = new AtomicInteger(0);

    private static final ExecutorService uploadService;

    private static final int maxConcurrency;

    private static final boolean enableRetry;

    private static final RetryPolicy retryPolicy;

    private static final FixSizedBuffers fixSizedBuffers;

    static
    {
        maxConcurrency = Integer.parseInt(ConfigFactory.Instance().getProperty("s3.client.service.threads"));

        //Fix https://github.com/pixelsdb/pixels/issues/1291
        uploadService = Executors.newFixedThreadPool(maxConcurrency, runnable -> {
            Thread thread = new Thread(runnable);
            thread.setDaemon(true);
            thread.setName(String.format("s3-upload-daemon-thread-%d", thread.getId()));
            return thread;
        });

        enableRetry = Boolean.parseBoolean(ConfigFactory.Instance().getProperty("read.request.enable.retry"));
        if (enableRetry)
        {
            int interval = Integer.parseInt(ConfigFactory.Instance().getProperty("read.request.retry.interval.ms"));
            retryPolicy = new RetryPolicy(interval);
        }
        else
        {
            retryPolicy = null;
        }

        fixSizedBuffers = new FixSizedBuffers(S3_BUFFER_SIZE);

        ShutdownHookManager.Instance().registerShutdownHook(S3OutputStream.class, true, () -> {
            uploadService.shutdownNow();
            fixSizedBuffers.clear();
        });
    }

    /**
     * Creates a new S3 OutputStream
     *
     * @param s3Client the AmazonS3 client
     * @param bucket   name of the bucket
     * @param key      path (key) within the bucket
     */
    public S3OutputStream(S3Client s3Client, String bucket, String key)
    {
        this(s3Client, bucket, key, S3_BUFFER_SIZE);
    }

    /**
     * Creates a new S3 OutputStream. Buffer size is specified.
     *
     * @param s3Client the AmazonS3 client.
     * @param bucket   name of the bucket.
     * @param key      path (key) within the bucket.
     * @param bufferSize the buffer size.
     */
    public S3OutputStream(S3Client s3Client, String bucket, String key, int bufferSize)
    {
        this.s3Client = s3Client;
        this.bucket = bucket;
        this.key = key;
        if (fixSizedBuffers.getBufferSize() == bufferSize)
        {
            this.buffer = fixSizedBuffers.allocate();
        }
        else
        {
            this.buffer = new byte[bufferSize];
        }
        this.position = 0;
        this.parts = new ConcurrentLinkedQueue<>();
        this.open = true;
    }

    /**
     * Write a byte array to the S3 output stream.
     *
     * @param buf the byte array to append
     */
    @Override
    public void write(final byte[] buf) throws IOException
    {
        write(buf, 0, buf.length);
    }

    @Override
    public void write(int b) throws IOException
    {
        this.assertOpen();
        if (position >= this.buffer.length)
        {
            flushBufferAndRewind();
        }
        this.buffer[position++] = (byte) b;
    }

    /**
     * Writes a byte array to the S3 Output Stream
     *
     * @param buf the byte array to write
     * @param off the offset into the array
     * @param len the number of bytes to write
     */
    @Override
    public void write(final byte[] buf, final int off, final int len) throws IOException
    {
        this.assertOpen();
        int offsetInBuf = off, remainToWrite = len;
        int remainInBuffer;
        while (remainToWrite > (remainInBuffer = this.buffer.length - position))
        {
            System.arraycopy(buf, offsetInBuf, this.buffer, this.position, remainInBuffer);
            this.position += remainInBuffer;
            flushBufferAndRewind();
            offsetInBuf += remainInBuffer;
            remainToWrite -= remainInBuffer;
        }
        System.arraycopy(buf, offsetInBuf, this.buffer, this.position, remainToWrite);
        this.position += remainToWrite;
    }

    /**
     * Flushes the buffer by uploading a part to S3.
     */
    @Override
    public synchronized void flush() throws IOException
    {
        this.assertOpen();
    }

    protected void flushBufferAndRewind() throws IOException
    {
        try
        {
            if (uploadId == null)
            {
                final CreateMultipartUploadRequest request = CreateMultipartUploadRequest.builder().bucket(this.bucket)
                        .key(this.key).acl(ObjectCannedACL.BUCKET_OWNER_FULL_CONTROL).build();
                CreateMultipartUploadResponse response = s3Client.createMultipartUpload(request);
                this.uploadId = response.uploadId();
            }
        } catch (Exception e)
        {
            throw new IOException("Failed to initiate multipart upload.", e);
        }
        uploadPart();
        this.position = 0;
    }

    protected void uploadPart()
    {
        while (this.concurrency.get() >= maxConcurrency)
        {
            try
            {
                TimeUnit.MILLISECONDS.sleep(10);
            } catch (InterruptedException e)
            {
                // do nothing
            }
        }
        this.concurrency.incrementAndGet();
        int partNumber = numParts.incrementAndGet();
        RequestBody requestBody = RequestBody.fromByteBuffer(ByteBuffer.wrap(buffer, 0, position));
        int length = position;
        CompletableFuture<CompletedPart> future = new CompletableFuture<>();
        uploadService.execute(() ->
        {
            UploadPartRequest uploadPartRequest = UploadPartRequest.builder()
                    .bucket(this.bucket)
                    .key(this.key)
                    .uploadId(this.uploadId)
                    .partNumber(partNumber).build();
            UploadExecutableRequest retryRequest = null;
            if (enableRetry)
            {
                retryRequest = new UploadExecutableRequest(
                        s3Client, uploadPartRequest, requestBody, length, partNumber, future, System.currentTimeMillis());
                retryPolicy.monitor(retryRequest);
            }
            UploadPartResponse uploadPartResponse = this.s3Client.uploadPart(uploadPartRequest, requestBody);
            if (enableRetry)
            {
                retryRequest.complete();
            }
            String etag = uploadPartResponse.eTag();
            CompletedPart part = CompletedPart.builder().partNumber(partNumber).eTag(etag).build();
            future.complete(part);
            this.concurrency.decrementAndGet();
        });
        this.parts.add(future);
    }

    @Override
    public void close() throws IOException
    {
        if (this.open)
        {
            this.open = false;
            if (this.uploadId != null)
            {
                if (this.position > 0)
                {
                    uploadPart();
                }
                List<CompletedPart> partList = new ArrayList<>(this.parts.size());
                for (CompletableFuture<CompletedPart> part : this.parts)
                {
                    try
                    {
                        partList.add(part.get());
                    } catch (Exception e)
                    {
                        throw new IOException("failed to execute the UploadPart request", e);
                    }
                }
                Collections.sort(partList, Comparator.comparingInt(CompletedPart::partNumber));
                CompletedMultipartUpload completedMultipartUpload = CompletedMultipartUpload.builder()
                        .parts(partList).build();
                CompleteMultipartUploadRequest completeMultipartUploadRequest =
                        CompleteMultipartUploadRequest.builder().bucket(this.bucket).key(this.key)
                                .uploadId(uploadId).multipartUpload(completedMultipartUpload).build();
                this.s3Client.completeMultipartUpload(completeMultipartUploadRequest);
            }
            else
            {
                final PutObjectRequest request = PutObjectRequest.builder().bucket(this.bucket).key(this.key)
                        .acl(ObjectCannedACL.BUCKET_OWNER_FULL_CONTROL).build();
                // Use DirectRequestBody instead of RequestBody to avoid memory copying
                this.s3Client.putObject(request, DirectRequestBody.fromBytesDirect(buffer, 0, position));
            }
            if (this.buffer.length == fixSizedBuffers.getBufferSize())
            {
                fixSizedBuffers.free(this.buffer);
            }
            this.buffer = null;
        }
    }

    /**
     * Should be called to cancel multipart upload when there is any exception.
     */
    public void cancel()
    {
        this.open = false;
        if (this.uploadId != null)
        {
            AbortMultipartUploadRequest request = AbortMultipartUploadRequest.builder()
                    .bucket(this.bucket).key(this.key).uploadId(this.uploadId).build();
            this.s3Client.abortMultipartUpload(request);
        }
    }

    private void assertOpen() throws IOException
    {
        if (!this.open)
        {
            throw new IOException("Stream closed");
        }
    }

    public static class UploadExecutableRequest implements RetryPolicy.ExecutableRequest
    {
        private final S3Client s3Client;
        private final UploadPartRequest request;
        private final RequestBody requestBody;
        private final int contentLength;
        private final int partNumber;
        private final CompletableFuture<CompletedPart> partCompleteFuture;
        private final long startTimeMs;
        private volatile long completeTimeMs = -1;
        private int retried = 0;

        public UploadExecutableRequest(
                S3Client s3Client, UploadPartRequest request, RequestBody requestBody, int contentLength,
                int partNumber, CompletableFuture<CompletedPart> partCompleteFuture, long startTimeMs)
        {
            this.s3Client = s3Client;
            this.request = request;
            this.requestBody = requestBody;
            this.contentLength = contentLength;
            this.partNumber = partNumber;
            this.partCompleteFuture = partCompleteFuture;
            this.startTimeMs = startTimeMs;
        }

        @Override
        public long getStartTimeMs()
        {
            return this.startTimeMs;
        }

        @Override
        public long getCompleteTimeMs()
        {
            return this.completeTimeMs;
        }

        @Override
        public int getLength()
        {
            return this.contentLength;
        }

        @Override
        public int getRetried()
        {
            return this.retried;
        }

        public void complete()
        {
            this.completeTimeMs = System.currentTimeMillis();
        }

        @Override
        public boolean execute()
        {
            if (partCompleteFuture.isDone())
            {
                return false;
            }
            logger.debug("retry UploadPart request: part number=" + this.partNumber + ", bucket=" +
                    this.request.bucket() + ", key=" + this.request.key());
            try
            {
                UploadPartResponse uploadPartResponse = this.s3Client.uploadPart(request, requestBody);
                String etag = uploadPartResponse.eTag();
                CompletedPart part = CompletedPart.builder().partNumber(partNumber).eTag(etag).build();
                this.completeTimeMs = System.currentTimeMillis();
                partCompleteFuture.complete(part);
            } catch (Exception e)
            {
                logger.error("Failed to execute UploadPart request");
            }
            finally
            {
                this.retried++;
            }
            return true;
        }
    }
}
