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
package io.pixelsdb.pixels.common.physical.impl;

import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.*;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * <p>
 * Referenced the implementation from
 * <a href="https://gist.github.com/blagerweij/ad1dbb7ee2fff8bcffd372815ad310eb">Barry Lagerweij</a>.
 * </p>
 * Created at: 9/24/21
 * Author: hank
 */
public class S3OutputStream extends OutputStream
{

    /**
     * Default chunk size is 10MB
     */
    protected static final int BUFFER_SIZE = 10 * 1024 * 1024;

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
    private final byte[] buf;

    /**
     * The position in the buffer
     */
    private int position;

    /**
     * Amazon S3 client.
     */
    private final S3AsyncClient s3Client;

    /**
     * The unique id for this upload
     */
    private String uploadId;

    /**
     * Collection of the etags for the parts that have been uploaded
     */
    private final List<CompletedPart> parts;

    /**
     * indicates whether the stream is still open / valid
     */
    private boolean open;

    /**
     * Creates a new S3 OutputStream
     *
     * @param s3Client the AmazonS3 client
     * @param bucket   name of the bucket
     * @param key      path (key) within the bucket
     */
    public S3OutputStream(S3AsyncClient s3Client, String bucket, String key)
    {
        this.s3Client = s3Client;
        this.bucket = bucket;
        this.key = key;
        this.buf = new byte[BUFFER_SIZE];
        this.position = 0;
        this.parts = new ArrayList<>();
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
     * @param byteArray the array to write
     * @param o         the offset into the array
     * @param l         the number of bytes to write
     */
    @Override
    public void write(final byte[] byteArray, final int o, final int l) throws IOException
    {
        this.assertOpen();
        int ofs = o, len = l;
        int size;
        while (len > (size = this.buf.length - position))
        {
            System.arraycopy(byteArray, ofs, this.buf, this.position, size);
            this.position += size;
            flushBufferAndRewind();
            ofs += size;
            len -= size;
        }
        System.arraycopy(byteArray, ofs, this.buf, this.position, len);
        this.position += len;
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
                CompletableFuture<CreateMultipartUploadResponse> response = s3Client.createMultipartUpload(request);
                this.uploadId = response.get().uploadId();
            }
        } catch (ExecutionException | InterruptedException e)
        {
            throw new IOException("Failed to initiate multipart upload.", e);
        }
        uploadPart();
        this.position = 0;
    }

    protected void uploadPart() throws IOException
    {
        UploadPartRequest request = UploadPartRequest.builder()
                .bucket(this.bucket)
                .key(this.key)
                .uploadId(this.uploadId)
                .partNumber(this.parts.size() + 1).build();
        CompletableFuture<UploadPartResponse> response =
                this.s3Client.uploadPart(request, AsyncRequestBody.fromByteBuffer(
                        ByteBuffer.wrap(buf, 0, position)));
        try
        {
            String etag = response.get().eTag();
            CompletedPart part = CompletedPart.builder().partNumber(this.parts.size() + 1).eTag(etag).build();
            this.parts.add(part);
        } catch (InterruptedException | ExecutionException e)
        {
            throw new IOException("Failed to upload part.", e);
        }
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
                CompletedMultipartUpload completedMultipartUpload = CompletedMultipartUpload.builder()
                        .parts(this.parts)
                        .build();
                CompleteMultipartUploadRequest completeMultipartUploadRequest =
                        CompleteMultipartUploadRequest.builder()
                                .bucket(this.bucket)
                                .key(this.key)
                                .uploadId(uploadId)
                                .multipartUpload(completedMultipartUpload)
                                .build();
                this.s3Client.completeMultipartUpload(completeMultipartUploadRequest).join();
            } else
            {
                final PutObjectRequest request = PutObjectRequest.builder().bucket(this.bucket).key(this.key)
                        .acl(ObjectCannedACL.BUCKET_OWNER_FULL_CONTROL).build();
                this.s3Client.putObject(request, AsyncRequestBody.fromByteBuffer(
                        ByteBuffer.wrap(buf, 0, position))).join();
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
            this.s3Client.abortMultipartUpload(request).join();
        }
    }

    @Override
    public void write(int b) throws IOException
    {
        this.assertOpen();
        if (position >= this.buf.length)
        {
            flushBufferAndRewind();
        }
        this.buf[position++] = (byte) b;
    }

    private void assertOpen()
    {
        if (!this.open)
        {
            throw new IllegalStateException("Closed");
        }
    }
}
