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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.io.ByteStreams;
import io.pixelsdb.pixels.common.turbo.cleanup.CleanupJournal;
import io.pixelsdb.pixels.storage.s3.S3;
import org.junit.Test;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.any;

public class TestCleanupS3Journal
{
    @Test
    public void largeOwnershipRecordUsesOneAtomicPutInsteadOfUndersizedMultipartParts() throws Exception
    {
        S3Client client = mock(S3Client.class);
        S3AsyncClient async = mock(S3AsyncClient.class);
        S3 storage = new S3(client, async);
        AtomicReference<String> written = new AtomicReference<>();
        when(client.putObject(any(PutObjectRequest.class), any(RequestBody.class))).thenAnswer(call ->
        {
            RequestBody body = call.getArgument(1);
            written.set(new String(ByteStreams.toByteArray(body.contentStreamProvider().newStream()), StandardCharsets.UTF_8));
            return PutObjectResponse.builder().build();
        });
        char[] content = new char[100000];
        Arrays.fill(content, 'x');
        JSONObject record = new JSONObject();
        record.put("largeRecord", new String(content));
        try
        {
            new CleanupJournal(storage, "s3://bucket/management/").write("intent.resources.json", record);
            verify(client).putObject(any(PutObjectRequest.class), any(RequestBody.class));
            verify(client, never()).createMultipartUpload(any(CreateMultipartUploadRequest.class));
            assertEquals(record, JSON.parseObject(written.get()));
        }
        finally { storage.close(); }
    }
}
