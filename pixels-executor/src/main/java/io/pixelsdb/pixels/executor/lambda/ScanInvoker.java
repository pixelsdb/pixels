/*
 * Copyright 2022 PixelsDB.
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
package io.pixelsdb.pixels.executor.lambda;

import com.alibaba.fastjson.JSON;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.lambda.model.InvocationType;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;

import java.util.concurrent.CompletableFuture;

/**
 * The lambda invoker for scan operator.
 *
 * @author hank
 * @date 4/18/22
 */
public class ScanInvoker
{
    private static final String SCAN_WORKER_NAME = "ScanWorker";

    private ScanInvoker() { }

    public static CompletableFuture<ScanOutput> invoke(ScanInput input)
    {
        String inputJson = JSON.toJSONString(input);
        SdkBytes payload = SdkBytes.fromUtf8String(inputJson);

        InvokeRequest request = InvokeRequest.builder()
                .functionName(SCAN_WORKER_NAME)
                .payload(payload)
                // using RequestResponse for higher function concurrency.
                .invocationType(InvocationType.REQUEST_RESPONSE)
                .build();

        return Lambda.Instance().getAsyncClient().invoke(request).handle((response, err) -> {
            if (err == null && response != null)
            {
                // 200 is the success status for RequestResponse invocation type.
                if(response.statusCode() == 200 && response.functionError() == null)
                {
                    String outputJson = response.payload().asUtf8String();
                    ScanOutput scanOutput = JSON.parseObject(outputJson, ScanOutput.class);
                    if (scanOutput == null)
                    {
                        throw new RuntimeException("failed to parse response payload, length=" +
                                response.payload().asByteArray().length);
                    }
                    return scanOutput;
                }
                else
                {
                    throw new RuntimeException("failed to execute the request, function error="
                            + response.functionError());
                }
            }
            throw new RuntimeException("failed to get response", err);
        });
    }
}
