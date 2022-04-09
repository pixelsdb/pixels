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

import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.storage.S3;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

/**
 * The physical reader for AWS S3.
 *
 * @author hank
 * Created at: 06/09/2021
 */
public class PhysicalS3Reader extends AbstractS3Reader
{
    /*
     * Most of the methods in this class are moved into AbstractS3Reader.
     */

    private static Logger logger = LogManager.getLogger(PhysicalS3Reader.class);
    private final static int LEN_1M = 1024*1024;
    private final static int LEN_10M = 1024*1024*10;
    private final static int ADAPTIVE_READ_TH = 2*1024*1024;

    private final S3AsyncClient asyncClient;
    private final S3AsyncClient asyncClient1M;
    private final S3AsyncClient asyncClient10M;

    public PhysicalS3Reader(Storage storage, String path) throws IOException
    {
        super(storage, path);
        if (! (storage instanceof S3))
        {
            throw new IOException("Storage is not S3.");
        }
        S3 s3 = (S3) this.s3;
        this.asyncClient = s3.getAsyncClient();
        if (S3.isRequestDiversionEnabled())
        {
            this.asyncClient1M = s3.getAsyncClient1M();
            this.asyncClient10M = s3.getAsyncClient10M();
        }
        else
        {
            this.asyncClient1M = this.asyncClient10M = null;
        }

        if (!useAsyncClient)
        {
            this.asyncClient.close();
        }
    }

    @Override
    public CompletableFuture<ByteBuffer> readAsync(long offset, int len) throws IOException
    {
        if (offset + len > this.length)
        {
            throw new IOException("Offset " + offset + " plus " +
                    len + " exceeds object length " + this.length + ".");
        }
        GetObjectRequest request = GetObjectRequest.builder().bucket(path.bucket)
                .key(path.key).range(toRange(offset, len)).build();
        CompletableFuture<ResponseBytes<GetObjectResponse>> future;
        if (useAsyncClient && len < ADAPTIVE_READ_TH)
        {
            if (S3.isRequestDiversionEnabled())
            {
                if (len < LEN_1M)
                {
                    future = asyncClient.getObject(request, AsyncResponseTransformer.toBytes());
                } else if (len < LEN_10M)
                {
                    future = asyncClient1M.getObject(request, AsyncResponseTransformer.toBytes());
                } else
                {
                    future = asyncClient10M.getObject(request, AsyncResponseTransformer.toBytes());
                }
            }
            else
            {
                future = asyncClient.getObject(request, AsyncResponseTransformer.toBytes());
            }
        }
        else
        {
            future = new CompletableFuture<>();
            clientService.execute(() -> {
                ResponseBytes<GetObjectResponse> response =
                        client.getObject(request, ResponseTransformer.toBytes());
                future.complete(response);
            });
        }

        try
        {
            /**
             * Issue #128:
             * We tried to use thenApplySync using the clientService executor,
             * it does not help improving the query performance.
             */
            return future.thenApply(resp ->
            {
                if (resp != null)
                {
                    return ByteBuffer.wrap(resp.asByteArrayUnsafe());
                }
                else
                {
                    logger.error("Failed complete the asynchronous read.");
                    return null;
                }
            });
        } catch (Exception e)
        {
            throw new IOException("Failed to read object.", e);
        }
    }

    @Override
    public void close() throws IOException
    {
        // Should not close the client because it is shared by all threads.
        // this.client.close(); // Closing s3 client may take several seconds.
    }
}
