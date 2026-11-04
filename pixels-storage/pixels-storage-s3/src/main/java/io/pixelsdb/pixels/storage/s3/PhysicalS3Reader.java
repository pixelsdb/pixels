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
package io.pixelsdb.pixels.storage.s3;

import io.pixelsdb.pixels.common.physical.Storage;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/** S3 range reads owned by a reader; shared clients remain storage-owned. */
public class PhysicalS3Reader extends AbstractS3Reader
{
    private static final Semaphore readSlots = new Semaphore(Math.max(1, AbstractS3.MaxRequestConcurrency), true);
    private final S3AsyncClient asyncClient;
    // Guarded by this; includes submissions not yet returned by the SDK and synchronous calls.
    private final Set<Read> reads = new HashSet<>();
    private volatile boolean closed;

    public PhysicalS3Reader(Storage storage, String path) throws IOException
    {
        super(storage, path);
        if (!(storage instanceof S3)) { throw new IOException("Storage is not S3."); }
        asyncClient = ((S3) storage).getS3AsyncClient();
    }

    @Override
    public boolean supportsAsyncRetry()
    {
        // SDK retries remain; scheduler-level copies would add overlapping requests.
        return false;
    }

    private void acquire() throws IOException
    {
        try
        {
            while (!closed)
            {
                if (readSlots.tryAcquire(100, TimeUnit.MILLISECONDS)) { return; }
            }
            throw new IOException("S3 reader is closed: " + pathStr);
        }
        catch (InterruptedException failure)
        {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted waiting for S3 read admission", failure);
        }
    }

    @Override
    public CompletableFuture<ByteBuffer> readAsync(long offset, int len) throws IOException
    {
        if (offset < 0 || len <= 0 || offset > length - len)
        {
            throw new IOException("Invalid S3 range: " + offset + "+" + len + "/" + length);
        }
        acquire();
        Read read = new Read(len);
        synchronized (this)
        {
            if (closed)
            {
                readSlots.release();
                throw new IOException("S3 reader is closed: " + pathStr);
            }
            reads.add(read);
            if (!useAsyncClient) { read.caller = Thread.currentThread(); }
            numRequests++;
        }
        try
        {
            if (read.result.isCancelled()) { throw new CancellationException("S3 read cancelled before submission"); }
            GetObjectRequest request = GetObjectRequest.builder().bucket(path.bucket).key(path.key)
                    .range(toRange(offset, len)).build();
            if (useAsyncClient)
            {
                // Register ownership before submission, without holding our lock across SDK code.
                read.source = asyncClient.getObject(request, new S3RangeResponseTransformer(offset, len));
                read.source.whenComplete(read::finish);
                if (read.result.isCancelled()) { read.cancelTransport(); }
            }
            else
            {
                // A synchronous request stays on its owning scanner, not a detached executor.
                read.finish(client.getObject(request, ResponseTransformer.toBytes()), null);
            }
        }
        catch (Exception failure) { read.finish(null, failure); }
        return read.result;
    }

    private final class Read
    {
        final int length;
        final CompletableFuture<ByteBuffer> result = new CompletableFuture<>();
        volatile CompletableFuture<ResponseBytes<GetObjectResponse>> source;
        Thread caller; // guarded by the enclosing reader

        Read(int length)
        {
            this.length = length;
            result.whenComplete((value, failure) -> {
                if (result.isCancelled()) { cancelTransport(); }
            });
        }

        void cancelTransport()
        {
            CompletableFuture<?> pending = source;
            if (pending != null) { pending.cancel(true); }
            synchronized (PhysicalS3Reader.this)
            {
                // Never interrupt a caller after it has left this read and moved to other work.
                if (reads.contains(this) && caller != null) { caller.interrupt(); }
            }
        }

        void cancel() { result.cancel(true); }

        void finish(ResponseBytes<GetObjectResponse> response, Throwable failure)
        {
            synchronized (PhysicalS3Reader.this)
            {
                if (!reads.remove(this)) { return; }
                caller = null;
                readSlots.release();
                PhysicalS3Reader.this.notifyAll();
            }
            // Complete user callbacks outside the ownership lock. Window owners separately
            // settle their buffer actions before freeing the window.
            if (failure != null) { result.completeExceptionally(failure); return; }
            try
            {
                byte[] bytes = response.asByteArrayUnsafe();
                if (bytes.length != length) { throw new IOException("Unexpected S3 range response length: " + bytes.length); }
                result.complete(ByteBuffer.wrap(bytes));
            }
            catch (Exception invalid) { result.completeExceptionally(invalid); }
        }
    }

    @Override
    public ByteBuffer readFully(int len) throws IOException
    {
        try
        {
            ByteBuffer value = readAsync(position, len).get();
            position += len;
            return value;
        }
        catch (InterruptedException failure)
        {
            try { close(); }
            catch (IOException closing) { failure.addSuppressed(closing); }
            finally { Thread.currentThread().interrupt(); }
            throw new IOException("Interrupted reading S3", failure);
        }
        catch (ExecutionException | CancellationException failure)
        {
            throw new IOException("Failed to read S3", failure);
        }
    }

    @Override
    public void close() throws IOException
    {
        Read[] pending;
        synchronized (this)
        {
            closed = true;
            pending = reads.toArray(new Read[0]);
        }
        for (Read read : pending) { read.cancel(); }
        boolean interrupted = Thread.interrupted();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        try
        {
            synchronized (this)
            {
                while (!reads.isEmpty())
                {
                    long remaining = deadline - System.nanoTime();
                    if (remaining <= 0) { throw new IOException("S3 requests did not settle: " + pathStr); }
                    try { TimeUnit.NANOSECONDS.timedWait(this, remaining); }
                    catch (InterruptedException failure) { interrupted = true; }
                }
            }
        }
        finally { if (interrupted) { Thread.currentThread().interrupt(); } }
        // SDK terminal state is not a transport-stop barrier: Netty schedules abort on
        // its event loop. Neither this wait nor readSlots is a hard bound on wire I/O.
    }
}
