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
package io.pixelsdb.pixels.server.grpc;

import io.grpc.stub.StreamObserver;
import io.pixelsdb.pixels.server.AmphiProto;
import io.pixelsdb.pixels.server.AmphiServiceGrpc;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * @author hank
 * @create 2023-04-24
 */
@GrpcService
public class AmphiServiceImpl extends AmphiServiceGrpc.AmphiServiceImplBase
{
    @Override
    public void sayHello(AmphiProto.HelloRequest request, StreamObserver<AmphiProto.HelloResponse> responseObserver)
    {
        String msg = "hello " + request.getName();
        AmphiProto.HelloResponse response = AmphiProto.HelloResponse.newBuilder().setMsg(msg).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
