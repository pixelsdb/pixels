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
import io.pixelsdb.pixels.common.exception.AmphiException;
import io.pixelsdb.pixels.amphi.AmphiProto;
import io.pixelsdb.pixels.amphi.AmphiServiceGrpc;
import net.devh.boot.grpc.server.service.GrpcService;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;

/**
 * @author hank
 * @create 2023-04-24
 */
@GrpcService
public class AmphiServiceImpl extends AmphiServiceGrpc.AmphiServiceImplBase
{
    @Autowired
    private AmphiExceptionAdvice amphiExceptionAdvice;

    @Override
    public void transpileSql(AmphiProto.TranspileSqlRequest request, StreamObserver<AmphiProto.TranspileSqlResponse> responseObserver)
    {
        SqlglotExecutor executor = new SqlglotExecutor();
        String sqlTranspiled = null;
        String errorMessage = null;
        int errorCode = 0;

        try {
            sqlTranspiled = executor.transpileSql(request.getSqlStatement(), request.getFromDialect(), request.getToDialect());
        } catch (AmphiException e) {
            errorMessage = amphiExceptionAdvice.handleAmphiException(e).getMessage();
            errorCode = 1;
        } catch (IOException | InterruptedException e) {
            errorMessage = e.getMessage();
            errorCode = 1;
        }

        AmphiProto.ResponseHeader.Builder headerBuilder = AmphiProto.ResponseHeader.newBuilder()
                .setErrorCode(errorCode)
                .setErrorMsg(errorMessage != null ? errorMessage : "");
        AmphiProto.TranspileSqlResponse.Builder responseBuilder = AmphiProto.TranspileSqlResponse.newBuilder();

        if (errorMessage != null) {
            responseBuilder.setHeader(headerBuilder);
        } else {
            responseBuilder.setSqlTranspiled(sqlTranspiled);
        }

        AmphiProto.TranspileSqlResponse response = responseBuilder.build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
