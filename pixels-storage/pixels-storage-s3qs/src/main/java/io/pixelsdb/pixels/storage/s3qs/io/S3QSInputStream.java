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
package io.pixelsdb.pixels.storage.s3qs.io;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.IOException;
import java.io.InputStream;

/**
 * This is the input stream for the small intermediate files stored in S3.
 * As the files are small (e.g, <= 8MB), we read all the file content at once into a buffer.
 */
public class S3QSInputStream extends InputStream
{

    /**
     * The temporary buffer used for storing the chunks
     */
    private byte[] buffer;

    /**
     * The position in the buffer
     */
    private int position;

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
        this.position = 0;
        this.open = true;
        // read data and populate buffer
        GetObjectRequest request = GetObjectRequest.builder().bucket(bucket).key(key).build();
        try
        {
            ResponseBytes<GetObjectResponse> responseBytes =
                    s3Client.getObject(request, ResponseTransformer.toBytes());
            // InputStream does not modify the buffer, thus it is safe to call asByteArrayUnsafe().
            this.buffer = responseBytes.asByteArrayUnsafe();
            this.position = 0;
        } catch (Exception e)
        {
            this.position = 0;
            throw new IOException("Failed to read object.", e);
        }
    }

    @Override
    public int read() throws IOException
    {
        this.assertOpen();
        if (position >= this.buffer.length)
        {
            // end of stream
            return -1;
        }
        return this.buffer[position++] & 0xFF;
    }

    @Override
    public int read(byte[] b) throws IOException
    {
        return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] buf, final int off, final int len) throws IOException
    {
        this.assertOpen();
        int offsetInBuf = off;
        int remainInBuffer = this.buffer.length - position;
        if (remainInBuffer >= len)
        {
            // The read can be served in buffer.
            System.arraycopy(this.buffer, this.position, buf, offsetInBuf, len);
            this.position += len;
            offsetInBuf += len;
        }
        else
        {
            // Read the remaining bytes in buffer.
            System.arraycopy(this.buffer, this.position, buf, offsetInBuf, remainInBuffer);
            this.position += remainInBuffer;
            offsetInBuf += remainInBuffer;
        }

        return offsetInBuf - off;
    }

    @Override
    public int available()
    {
        if (this.buffer != null)
        {
            return this.buffer.length - this.position;
        }
        return 0;
    }

    @Override
    public void close()
    {
        if (this.open)
        {
            this.open = false;
            this.buffer = null;
            // Don't close s3Client as it is external.
        }
    }

    private void assertOpen() throws IOException
    {
        if (!this.open)
        {
            throw new IOException("Stream closed");
        }
    }
}
