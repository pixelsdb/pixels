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
package io.pixelsdb.pixels.storage.sqs3.io;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.IOException;
import java.io.InputStream;

public class S3QSInputStream extends InputStream
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
    private byte[] buffer;

    /**
     * The position in the buffer
     */
    private int bufferPosition;

    /**
     * Amazon S3 client.
     */
    private final S3Client s3Client;

    /**
     * indicates whether the stream is still open / valid
     */
    private boolean open;

    /**
     * Creates a new S3 InputStream
     *
     * @param s3Client the AmazonS3 client
     * @param bucket   name of the bucket
     * @param key      path (key) within the bucket
     */
    public S3QSInputStream(S3Client s3Client, String bucket, String key) throws IOException
    {
        this.s3Client = s3Client;
        this.bucket = bucket;
        this.key = key;
        this.bufferPosition = 0;
        this.open = true;
    }

    @Override
    public int read() throws IOException
    {
        this.assertOpen();
        if (this.buffer == null)
        {
            if (populateBuffer() < 0)
            {
                return -1;
            }
        }
        if (bufferPosition >= this.buffer.length)
        {
            return -1;
        }
        return this.buffer[bufferPosition++] & 0xFF;
    }

    @Override
    public int read(byte[] b) throws IOException
    {
        return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] buf, int off, int len) throws IOException
    {
        this.assertOpen();
        if (this.buffer == null)
        {
            // try to populate the buffer for the first time or after exception or EOF.
            if (populateBuffer() < 0)
            {
                return -1;
            }
        }
        int offsetInBuf = off, remainToRead = len;
        int remainInBuffer = this.buffer.length - bufferPosition;
        if (remainInBuffer >= remainToRead)
        {
            // The read can be served in buffer.
            System.arraycopy(this.buffer, this.bufferPosition, buf, offsetInBuf, remainToRead);
            this.bufferPosition += remainToRead;
            offsetInBuf += remainToRead;
        }
        else
        {
            // Read the remaining bytes in buffer.
            System.arraycopy(this.buffer, this.bufferPosition, buf, offsetInBuf, remainInBuffer);
            this.bufferPosition += remainInBuffer;
            offsetInBuf += remainInBuffer;
        }

        return offsetInBuf - off;
    }

    /**
     * Populate the read buffer.
     * @return the bytes been populated into the read buffer, -1 if reaches EOF.
     * @throws IOException
     */
    protected int populateBuffer() throws IOException
    {
        GetObjectRequest request = GetObjectRequest.builder().bucket(this.bucket).key(this.key).build();
        ResponseBytes<GetObjectResponse> responseBytes = this.s3Client.getObject(request, ResponseTransformer.toBytes());
        try
        {
            this.buffer = responseBytes.asByteArray();
            this.bufferPosition = 0;
            return this.buffer.length;
        } catch (Exception e)
        {
            this.buffer = null;
            this.bufferPosition = 0;
            throw new IOException("Failed to read object.", e);
        }
    }

    @Override
    public int available() throws IOException
    {
        if (this.buffer != null)
        {
            return this.buffer.length - this.bufferPosition;
        }
        return 0;
    }

    @Override
    public void close() throws IOException
    {
        if (this.open)
        {
            this.open = false;
            // Don't close s3Client as it is external.
        }
    }

    private void assertOpen()
    {
        if (!this.open)
        {
            throw new IllegalStateException("Closed");
        }
    }
}
