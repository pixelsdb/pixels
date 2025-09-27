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

import io.pixelsdb.pixels.common.physical.FixSizedBuffers;
import io.pixelsdb.pixels.storage.s3.io.DirectRequestBody;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.IOException;
import java.io.OutputStream;

import static io.pixelsdb.pixels.common.utils.Constants.S3QS_BUFFER_SIZE;

public class S3QSOutputStream extends OutputStream
{
    private static final Logger logger = LogManager.getLogger(S3QSOutputStream.class);

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
     * indicates whether the stream is still open / valid
     */
    private boolean open;

    private static final FixSizedBuffers fixSizedBuffers;

    static
    {
        fixSizedBuffers = new FixSizedBuffers(S3QS_BUFFER_SIZE);
        Runtime.getRuntime().addShutdownHook(new Thread(fixSizedBuffers::clear));
    }

    public S3QSOutputStream(S3Client s3Client, String bucket, String key, int bufferSize)
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
        this.open = true;
    }

    /**
     * Write a byte array to the http output stream.
     *
     * @param b the byte array to write
     * @throws IOException
     */
    @Override
    public void write(byte[] b) throws IOException
    {
        write(b, 0, b.length);
    }

    @Override
    public void write(int b) throws IOException
    {
        this.assertOpen();
        if (position >= this.buffer.length)
        {
            throw new IOException("Buffer is full");
        }
        this.buffer[position++] = (byte) b;
    }

    @Override
    public void write(final byte[] buf, final int off, final int len) throws IOException
    {
        this.assertOpen();
        int remainInBuffer = this.buffer.length - position;
        if (len > remainInBuffer)
        {
            throw new IOException("Buffer is full");
        }
        System.arraycopy(buf, off, this.buffer, this.position, len);
        this.position += len;
    }

    @Override
    public void flush() throws IOException
    {
        this.assertOpen();
    }

    @Override
    public void close() throws IOException
    {
        if (this.open)
        {
            this.open = false;
            final PutObjectRequest request = PutObjectRequest.builder().bucket(this.bucket).key(this.key)
                    .acl(ObjectCannedACL.BUCKET_OWNER_FULL_CONTROL).build();
            // Use DirectRequestBody instead of RequestBody to avoid memory copying
            this.s3Client.putObject(request, DirectRequestBody.fromBytesDirect(buffer, 0, position));
            // release buffer
            if (this.buffer.length == fixSizedBuffers.getBufferSize())
            {
                fixSizedBuffers.free(this.buffer);
            }
            this.buffer = null;
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
