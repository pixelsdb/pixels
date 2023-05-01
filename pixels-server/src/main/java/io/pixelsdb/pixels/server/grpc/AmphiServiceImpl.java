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

import java.io.IOException;

/**
 * @author hank
 * @create 2023-04-24
 */
@GrpcService
public class AmphiServiceImpl extends AmphiServiceGrpc.AmphiServiceImplBase
{
    @Override
    public void transpileSql(AmphiProto.TranspileSqlRequest request, StreamObserver<AmphiProto.TranspileSqlResponse> responseObserver)
    {
        SqlglotExecutor executor = new SqlglotExecutor();
        String sqlTranspiled = null;

        try {
            sqlTranspiled = executor.transpileSql(request.getSqlStatement(), request.getFromDialect(), request.getToDialect());
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        AmphiProto.TranspileSqlResponse response = AmphiProto.TranspileSqlResponse.newBuilder().setSqlTranspiled(sqlTranspiled).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
