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
package io.pixelsdb.pixels.planner.coordinate;

import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.planner.plan.physical.domain.*;
import io.pixelsdb.pixels.storage.s3qs.S3QS;
import org.junit.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.any;

public class TestS3QSResourceFailureLifecycle
{
    @Test
    public void partialDeleteTracksRemainingPrefixAndOwnedQueue() throws Exception
    {
        SqsClient sqs = mock(SqsClient.class);
        S3Client s3 = mock(S3Client.class);
        S3QS storage = new S3QS(s3, sqs, 30);
        when(sqs.getQueueUrl(any(GetQueueUrlRequest.class)))
                .thenReturn(GetQueueUrlResponse.builder().queueUrl("owned-url").build());
        when(sqs.deleteQueue(any(DeleteQueueRequest.class))).thenThrow(SqsException.builder().message("denied").build())
                .thenReturn(DeleteQueueResponse.builder().build());
        when(s3.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(ListObjectsV2Response.builder()
                .keyCount(1).isTruncated(false).contents(S3Object.builder().key("query/file").size(1L).build()).build());
        when(s3.deleteObjects(any(DeleteObjectsRequest.class)))
                .thenReturn(DeleteObjectsResponse.builder().errors(S3Error.builder().key("query/file").code("AccessDenied").build()).build())
                .thenReturn(DeleteObjectsResponse.builder().build());
        AtomicReference<Map<String, Object>> snapshot = new AtomicReference<>();
        S3QSShuffleResourceLifecycle lifecycle = new S3QSShuffleResourceLifecycle(storage);
        lifecycle.setProgressListener(snapshot::set);
        lifecycle.prepare(Collections.singletonList(info(null)));
        try { lifecycle.cleanup(); fail("partial deletion must not report cleaned"); }
        catch (IOException expected) { assertTrue(expected.getSuppressed().length > 0); }
        assertEquals(false, snapshot.get().get("cleaned"));
        assertEquals(Collections.singletonList("bucket/query/"), snapshot.get().get("remainingOwnedObjectPrefixes"));
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> queues = (List<Map<String, Object>>) snapshot.get().get("ownedQueues");
        assertEquals(false, queues.get(0).get("deleted"));
        // Explicit retry, not an implicit retry loop. Only remaining resources are touched.
        lifecycle.cleanup();
        lifecycle.cleanup();
        assertEquals(true, snapshot.get().get("cleaned"));
        verify(sqs, times(2)).deleteQueue(any(DeleteQueueRequest.class));
        verify(s3, times(2)).deleteObjects(any(DeleteObjectsRequest.class));
        storage.close();
    }

    @Test
    public void queueCreatedBeforeUrlLookupFailureIsStillRecoverableByName() throws Exception
    {
        SqsClient sqs = mock(SqsClient.class);
        S3Client s3 = mock(S3Client.class);
        S3QS storage = new S3QS(s3, sqs, 30);
        SqsException.Builder failure = SqsException.builder();
        failure.message("URL lookup failed after create");
        SqsException original = (SqsException) failure.build();
        when(sqs.getQueueUrl(any(GetQueueUrlRequest.class))).thenThrow(original)
                .thenReturn(GetQueueUrlResponse.builder().queueUrl("recovered-url").build());
        when(s3.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(ListObjectsV2Response.builder().keyCount(0).build());
        AtomicReference<Map<String, Object>> snapshot = new AtomicReference<>();
        S3QSShuffleResourceLifecycle lifecycle = new S3QSShuffleResourceLifecycle(storage);
        lifecycle.setProgressListener(snapshot::set);
        try { lifecycle.prepare(Collections.singletonList(info(null))); fail("prepare must fail"); }
        catch (IOException expected) { assertSame(original, expected.getCause()); }
        verify(sqs).deleteQueue(DeleteQueueRequest.builder().queueUrl("recovered-url").build());
        assertEquals(true, snapshot.get().get("cleaned"));
        storage.close();
    }

    @Test
    public void externallyProvidedQueueIsNeverOwnedOrDeleted() throws Exception
    {
        SqsClient sqs = mock(SqsClient.class);
        S3Client s3 = mock(S3Client.class);
        S3QS storage = new S3QS(s3, sqs, 30);
        when(s3.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(ListObjectsV2Response.builder().keyCount(0).build());
        S3QSShuffleResourceLifecycle lifecycle = new S3QSShuffleResourceLifecycle(storage);
        lifecycle.prepare(Collections.singletonList(info("shared-url")));
        lifecycle.cleanup();
        verify(sqs, never()).createQueue(any(CreateQueueRequest.class));
        verify(sqs, never()).deleteQueue(any(DeleteQueueRequest.class));
        storage.close();
    }

    private static ShuffleInfo info(String url)
    {
        return new ShuffleInfo("query", new StorageInfo(Storage.Scheme.s3qs, null, null, null, null),
                "bucket/query/", 1, 1, 1, 1, Collections.singletonList(new ShuffleQueueInfo(0, "query-p0", url)));
    }
}
