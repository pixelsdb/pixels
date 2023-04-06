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
package io.pixelsdb.pixels.lambda.invoker;

import com.alibaba.fastjson.JSON;
import io.pixelsdb.pixels.common.turbo.Input;
import io.pixelsdb.pixels.common.turbo.Invoker;
import io.pixelsdb.pixels.common.turbo.Output;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.lambda.model.GetFunctionConfigurationRequest;
import software.amazon.awssdk.services.lambda.model.GetFunctionConfigurationResponse;
import software.amazon.awssdk.services.lambda.model.InvocationType;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;

import java.util.concurrent.CompletableFuture;

/**
 * The invoker for all kinds of cloud function workers.
 * @author hank
 * @date 28/06/2022
 */
public abstract class LambdaInvoker implements Invoker
{
    private static final Logger logger = LogManager.getLogger(LambdaInvoker.class);

    private final String functionName;
    private final int memoryMB;

    protected LambdaInvoker(String functionName)
    {
        this.functionName = functionName;
        GetFunctionConfigurationResponse response = Lambda.Instance().getAsyncClient().getFunctionConfiguration(
                GetFunctionConfigurationRequest.builder().functionName(functionName).build()).join();
        this.memoryMB = response.memorySize();
        logger.info("function '" + this.functionName + "' memory size: " + this.memoryMB);
    }

    public final CompletableFuture<Output> invoke(Input input)
    {
        String inputJson = JSON.toJSONString(input);
        SdkBytes payload = SdkBytes.fromUtf8String(inputJson);

        InvokeRequest request = InvokeRequest.builder()
                .functionName(this.functionName)
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
                    Output output = this.parseOutput(outputJson);
                    if (output == null)
                    {
                        throw new RuntimeException("failed to parse response payload, length=" +
                                response.payload().asByteArray().length);
                    }
                    output.setMemoryMB(this.memoryMB);
                    return output;
                }
                else
                {
                    throw new RuntimeException("failed to execute the request, function error (" +
                            response.statusCode() + "): " + response.functionError());
                }
            }
            throw new RuntimeException("failed to get response", err);
        });
    }

    @Override
    public String getFunctionName()
    {
        return functionName;
    }

    @Override
    public int getMemoryMB()
    {
        return memoryMB;
    }
}
