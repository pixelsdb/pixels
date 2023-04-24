/*
 * Copyright 2023 PixelsDB.
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
package io.pixelsdb.pixels.server;

import net.devh.boot.grpc.client.inject.GrpcClient;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author hank
 * @create 2023-04-24
 */
@SpringBootTest(properties = {
        "grpc.server.in-process-name=test", // Enable inProcess server
        "grpc.server.port=-1", // Disable external server
        "grpc.client.mock.address=in-process:test" // Configure the client to connect to the inProcess server
})
@DirtiesContext // Ensures that the grpc-server is properly shutdown after each test, avoiding "port already in use".
public class TestAmphiAPI
{
    @GrpcClient("mock")
    private AmphiServiceGrpc.AmphiServiceBlockingStub amphiService;

    @Test
    @DirtiesContext
    public void testSayHello()
    {
        AmphiProto.HelloRequest request = AmphiProto.HelloRequest.newBuilder().setName("pixels-amphi").build();
        AmphiProto.HelloResponse response = amphiService.sayHello(request);
        assertNotNull(response);
        assertEquals("hello pixels-amphi", response.getMsg());
    }
}
