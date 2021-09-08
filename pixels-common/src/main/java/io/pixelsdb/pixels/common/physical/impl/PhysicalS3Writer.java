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

import io.pixelsdb.pixels.common.physical.PhysicalWriter;
import io.pixelsdb.pixels.common.physical.Storage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import static io.pixelsdb.pixels.common.utils.Constants.S3_BUFFER_SIZE;

/**
 * Created at: 06/09/2021
 * Author: hank
 */
public class PhysicalS3Writer implements PhysicalWriter
{
    private static Logger logger = LogManager.getLogger(PhysicalS3Writer.class);

    private S3 s3;
    private S3.Path path;
    private String pathStr;
    private long position;
    private S3AsyncClient client;
    private File tempFile;
    private OutputStream out;

    public PhysicalS3Writer(Storage storage, String path) throws IOException
    {
        if (storage instanceof S3)
        {
            this.s3 = (S3) storage;
        }
        else
        {
            throw new IOException("Storage is not S3.");
        }
        if (path.startsWith("s3://"))
        {
            // remove the scheme.
            path = path.substring(5);
        }
        this.path = new S3.Path(path);
        this.pathStr = path;
        this.position = 0L;
        this.client = s3.getClient();
        this.s3.create(path, false, S3_BUFFER_SIZE, (short)1);
        this.tempFile = File.createTempFile("pixels-s3-", ".tmp");
        this.out = new FileOutputStream(tempFile);
    }

    /**
     * Prepare the writer to ensure the length can fit into current block.
     *
     * @param length length of content
     * @return starting offset after preparing. If -1, means prepare has failed,
     * due to the specified length cannot fit into current block.
     */
    @Override
    public long prepare(int length) throws IOException
    {
        return position;
    }

    /**
     * Append content to the file.
     *
     * @param buffer content buffer
     * @return start offset of content in the file.
     */
    @Override
    public long append(ByteBuffer buffer) throws IOException
    {
        buffer.flip();
        int length = buffer.remaining();
        return append(buffer.array(), buffer.arrayOffset() + buffer.position(), length);
    }

    /**
     * Append content to the file.
     *
     * @param buffer content buffer container
     * @param offset start offset of actual content buffer
     * @param length length of actual content buffer
     * @return start offset of content in the file.
     */
    @Override
    public long append(byte[] buffer, int offset, int length) throws IOException
    {
        long start = position;
        this.out.write(buffer, offset, length);
        position += length;
        return start;
    }

    /**
     * Close writer.
     */
    @Override
    public void close() throws IOException
    {
        this.out.close();
        PutObjectRequest request = PutObjectRequest.builder()
                .bucket(path.bucket).key(path.key).build();
        try
        {
            this.client.putObject(request, this.tempFile.toPath()).get();
        } catch (Exception e)
        {
            throw new IOException("Failed to put local temp file to S3.", e);
        }
        this.tempFile.deleteOnExit();
        this.client.close();
    }

    /**
     * Flush writer.
     */
    @Override
    public void flush() throws IOException
    {
        this.out.flush();
    }

    @Override
    public String getPath()
    {
        return pathStr;
    }
}
