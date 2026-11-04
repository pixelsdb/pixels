/*
 * Copyright 2026 PixelsDB.
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
package io.pixelsdb.pixels.storage.s3qs;

import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.turbo.cleanup.CleanupDriver;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;
import java.io.IOException;

/** Resolve by the pre-recorded queue name, including creates whose response was lost. */
public final class S3QSQueueCleanupDriver implements CleanupDriver
{
    private final SqsClient sqs;

    public S3QSQueueCleanupDriver() { this.sqs = null; }
    public S3QSQueueCleanupDriver(SqsClient sqs) { this.sqs = java.util.Objects.requireNonNull(sqs); }

    public String getType() { return "s3qs-queue"; }

    private SqsClient client() throws IOException
    { return sqs != null ? sqs : ((S3QS) StorageFactory.Instance().getStorage(Storage.Scheme.s3qs)).getSqsClient(); }

    public void delete(String name) throws IOException
    {
        try
        {
            SqsClient client = client();
            String url = client.getQueueUrl(GetQueueUrlRequest.builder().queueName(name).build()).queueUrl();
            client.deleteQueue(DeleteQueueRequest.builder().queueUrl(url).build());
        }
        catch (QueueDoesNotExistException ignored) { /* Idempotent after partial cleanup or response loss. */ }
        catch (SqsException failure) { throw new IOException("failed to delete queue " + name, failure); }
    }

    public boolean exists(String name) throws IOException
    {
        try
        {
            client().getQueueUrl(GetQueueUrlRequest.builder().queueName(name).build());
            return true;
        }
        catch (QueueDoesNotExistException ignored) { return false; }
        catch (SqsException failure) { throw new IOException("failed to check queue " + name, failure); }
    }
}
