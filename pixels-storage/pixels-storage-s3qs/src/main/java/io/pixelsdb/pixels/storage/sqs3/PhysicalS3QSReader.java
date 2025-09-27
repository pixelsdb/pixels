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
package io.pixelsdb.pixels.storage.sqs3;

import io.pixelsdb.pixels.common.physical.ObjectPath;
import io.pixelsdb.pixels.common.physical.PhysicalReader;
import io.pixelsdb.pixels.common.physical.Storage;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;

/**
 * This is the reader for small intermediate files on S3.
 * The file is fully read at once in the constructor of this class.
 */
public class PhysicalS3QSReader implements PhysicalReader
{
    private final S3QS s3qs;
    private final ObjectPath path;
    private final String pathStr;
    private final byte[] buffer;
    private final int length;
    private int position;

    public PhysicalS3QSReader(Storage storage, String path) throws IOException
    {
        if (storage instanceof S3QS)
        {
            this.s3qs = (S3QS) storage;
        } else
        {
            throw new IOException("Storage is not S3QS.");
        }
        if (path.contains("://"))
        {
            // remove the scheme.
            path = path.substring(path.indexOf("://") + 3);
        }
        this.path = new ObjectPath(path);
        this.pathStr = path;
        this.position = 0;
        // read data and populate buffer
        GetObjectRequest request = GetObjectRequest.builder().bucket(this.path.bucket).key(this.path.key).build();
        try
        {
            ResponseBytes<GetObjectResponse> responseBytes =
                    this.s3qs.getS3Client().getObject(request, ResponseTransformer.toBytes());
            // PhysicalReader does not modify the buffer, thus it is safe to call asByteArrayUnsafe().
            this.buffer = responseBytes.asByteArrayUnsafe();
            this.length = this.buffer.length;
            this.position = 0;
        } catch (Exception e)
        {
            this.position = 0;
            throw new IOException("Failed to read object.", e);
        }
    }
    @Override
    public long getFileLength() throws IOException
    {
        return this.length;
    }

    @Override
    public void seek(long desired) throws IOException
    {
        if (0 <= desired && desired < length)
        {
            position = (int) desired;
            return;
        }
        throw new IOException("Desired offset " + desired + " is out of bound (" + 0 + "," + length + ")");
    }

    @Override
    public ByteBuffer readFully(int len) throws IOException
    {
        if (this.position + len > this.length)
        {
            throw new IOException("Current position " + this.position + " plus " +
                    len + " exceeds object length " + this.length + ".");
        }
        ByteBuffer byteBuffer = ByteBuffer.wrap(this.buffer, this.position, len);
        this.position += len;
        return byteBuffer;
    }

    @Override
    public void readFully(byte[] buffer) throws IOException
    {
        readFully(buffer, 0, buffer.length);
    }

    @Override
    public void readFully(byte[] buffer, int off, int len) throws IOException
    {
        if (this.position + len > this.length)
        {
            throw new IOException("Current position " + this.position + " plus " +
                    len + " exceeds object length " + this.length + ".");
        }
        System.arraycopy(this.buffer, this.position, buffer, off, len);
        this.position += len;
    }

    @Override
    public boolean supportsAsync()
    {
        return false;
    }

    @Override
    public CompletableFuture<ByteBuffer> readAsync(long offset, int length) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long readLong(ByteOrder byteOrder) throws IOException
    {
        ByteBuffer buffer = readFully(Long.BYTES).order(requireNonNull(byteOrder));
        return buffer.getLong();
    }

    @Override
    public int readInt(ByteOrder byteOrder) throws IOException
    {
        ByteBuffer buffer = readFully(Integer.BYTES).order(requireNonNull(byteOrder));
        return buffer.getInt();
    }

    @Override
    public void close()
    {
        // nothing to close.
    }

    @Override
    public String getPath()
    {
        return pathStr;
    }

    @Override
    public String getPathUri() throws IOException
    {
        return this.s3qs.ensureSchemePrefix(pathStr);
    }

    @Override
    public String getName()
    {
        return path.key;
    }

    /**
     * @return -1 as there no valid block id for intermediate files
     */
    @Override
    public long getBlockId() throws IOException
    {
        return -1;
    }

    @Override
    public Storage.Scheme getStorageScheme()
    {
        return this.s3qs.getScheme();
    }

    /**
     * @return 1 as the file is read by a single get object request
     */
    @Override
    public int getNumReadRequests()
    {
        return 1;
    }
}
