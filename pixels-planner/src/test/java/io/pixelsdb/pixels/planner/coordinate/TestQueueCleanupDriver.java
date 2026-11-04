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

import io.pixelsdb.pixels.storage.s3qs.S3QSQueueCleanupDriver;
import org.junit.Test;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;
import java.io.IOException;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.any;

public class TestQueueCleanupDriver
{
    @Test
    public void missingQueueIsAnIdempotentSuccess() throws Exception
    {
        SqsClient client = mock(SqsClient.class);
        when(client.getQueueUrl(any(GetQueueUrlRequest.class)))
                .thenThrow(QueueDoesNotExistException.builder().message("missing").build());
        S3QSQueueCleanupDriver driver = new S3QSQueueCleanupDriver(client);
        assertFalse(driver.exists("owned-queue"));
        driver.delete("owned-queue");
        verify(client, never()).deleteQueue(any(DeleteQueueRequest.class));
    }

    @Test
    public void deleteAcknowledgementIsIndependentOfAbsence() throws Exception
    {
        SqsClient client = mock(SqsClient.class);
        when(client.getQueueUrl(any(GetQueueUrlRequest.class)))
                .thenReturn(GetQueueUrlResponse.builder().queueUrl("owned-url").build());
        S3QSQueueCleanupDriver driver = new S3QSQueueCleanupDriver(client);
        driver.delete("owned-queue");
        assertTrue(driver.exists("owned-queue"));
        verify(client).deleteQueue(DeleteQueueRequest.builder().queueUrl("owned-url").build());
    }

    @Test
    public void permissionAndTransportErrorsAreNotAbsenceEvidence() throws Exception
    {
        SqsClient client = mock(SqsClient.class);
        when(client.getQueueUrl(any(GetQueueUrlRequest.class)))
                .thenThrow(SqsException.builder().message("AccessDenied").statusCode(403).build());
        S3QSQueueCleanupDriver driver = new S3QSQueueCleanupDriver(client);
        try { driver.exists("owned-queue"); fail("access failure is not absence"); }
        catch (IOException expected) { assertTrue(expected.getCause() instanceof SqsException); }
    }
}
