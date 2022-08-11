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
package io.pixelsdb.pixels.common.physical.io;

import io.pixelsdb.pixels.common.utils.ConfigFactory;
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
 * Created at: 9/24/21
 */
public class S3OutputStream extends OutputStream
{
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
    private final byte[] buffer;

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
     * indicates whether the stream is still open / valid
     */
    private boolean open;

    /**
     * The number of UploadPart requests that are executing
     */
    private final AtomicInteger concurrency = new AtomicInteger(0);

    private static final ExecutorService uploadService;

    private static final int maxConcurrency;

    static
    {
        maxConcurrency = Integer.parseInt(ConfigFactory.Instance().getProperty("s3.client.service.threads"));
        uploadService = Executors.newFixedThreadPool(maxConcurrency);
        Runtime.getRuntime().addShutdownHook(new Thread(uploadService::shutdownNow));
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
        this.buffer = new byte[bufferSize];
        this.position = 0;
        this.parts = new ConcurrentLinkedQueue<>();
        this.open = true;
    }

    /**
     * Write an array to the S3 output stream.
     *
     * @param b the byte-array to append
     */
    @Override
    public void write(byte[] b) throws IOException
    {
        write(b, 0, b.length);
    }

    /**
     * Writes an array to the S3 Output Stream
     *
     * @param buf the array to write
     * @param off the offset into the array
     * @param len the number of bytes to write
     */
    @Override
    public void write(final byte[] buf, final int off, final int len) throws IOException
    {
        this.assertOpen();
        int offsetInBuf = off, remainToRead = len;
        int remainInBuffer;
        while (remainToRead > (remainInBuffer = this.buffer.length - position))
        {
            System.arraycopy(buf, offsetInBuf, this.buffer, this.position, remainInBuffer);
            this.position += remainInBuffer;
            flushBufferAndRewind();
            offsetInBuf += remainInBuffer;
            remainToRead -= remainInBuffer;
        }
        System.arraycopy(buf, offsetInBuf, this.buffer, this.position, remainToRead);
        this.position += remainToRead;
    }

    /**
     * Flushes the buffer by uploading a part to S3.
     */
    @Override
    public synchronized void flush()
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

    protected void uploadPart() throws IOException
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
        CompletableFuture<CompletedPart> future = new CompletableFuture<>();
        uploadService.execute(() ->
        {
            UploadPartRequest uploadPartRequest = UploadPartRequest.builder()
                    .bucket(this.bucket)
                    .key(this.key)
                    .uploadId(this.uploadId)
                    .partNumber(partNumber).build();
            UploadPartResponse uploadPartResponse = this.s3Client.uploadPart(uploadPartRequest, requestBody);
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
            } else
            {
                final PutObjectRequest request = PutObjectRequest.builder().bucket(this.bucket).key(this.key)
                        .acl(ObjectCannedACL.BUCKET_OWNER_FULL_CONTROL).build();
                this.s3Client.putObject(request, RequestBody.fromByteBuffer(ByteBuffer.wrap(buffer, 0, position)));
            }
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

    private void assertOpen()
    {
        if (!this.open)
        {
            throw new IllegalStateException("Closed");
        }
    }
}
