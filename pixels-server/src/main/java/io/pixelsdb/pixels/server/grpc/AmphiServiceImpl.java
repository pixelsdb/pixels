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
import io.pixelsdb.pixels.server.AmphiProto;
import io.pixelsdb.pixels.server.AmphiServiceGrpc;
import net.devh.boot.grpc.server.service.GrpcService;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.sql.*;

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

    @Override
    public void trinoQuery(AmphiProto.TrinoQueryRequest request, StreamObserver<AmphiProto.TrinoQueryResponse> responseObserver) {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        String rsStr = "";

        String errorMessage = null;
        int errorCode = 0;

        try {
            Class.forName("io.trino.jdbc.TrinoDriver");
            String trinoEndpoint = String.format("jdbc:trino://%s:%d/%s/%s",
                    request.getTrinoUrl(),
                    request.getTrinoPort(),
                    request.getCatalog(),
                    request.getSchema());
            conn = DriverManager.getConnection( trinoEndpoint, "pixels", "");

            stmt = conn.createStatement();
            rs = stmt.executeQuery(request.getSqlQuery());
            rsStr = resultSetToString(rs);
        } catch(Exception e) {
            errorMessage = e.getMessage();
            errorCode = 1;
        } finally {
            try {
                if(rs != null) rs.close();
                if(stmt != null) stmt.close();
                if(conn != null) conn.close();
            } catch(Exception e) {
                errorMessage = errorMessage + e.getMessage();
            }
        }

        AmphiProto.ResponseHeader.Builder headerBuilder = AmphiProto.ResponseHeader.newBuilder()
                .setErrorCode(errorCode)
                .setErrorMsg(errorMessage != null ? errorMessage : "");
        AmphiProto.TrinoQueryResponse.Builder responseBuilder = AmphiProto.TrinoQueryResponse.newBuilder();

        if (errorMessage != null) {
            responseBuilder.setHeader(headerBuilder);
        } else {
            responseBuilder.setQueryResult(rsStr);
        }

        AmphiProto.TrinoQueryResponse response = responseBuilder.build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    public String resultSetToString(ResultSet resultSet) throws SQLException
    {
        StringBuilder sb = new StringBuilder();
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();

        while (resultSet.next()) {
            for (int i = 1; i <= columnCount; i++) {
                if (i > 1) sb.append(", ");
                String columnName = metaData.getColumnName(i);
                String value = resultSet.getString(i);

                sb.append(columnName).append(": ").append(value);
            }
            sb.append("\n");
        }

        return sb.toString();
    }

}