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

package io.pixelsdb.pixels.core.retina;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.pixelsdb.pixels.cache.MemoryMappedFile;
import io.pixelsdb.pixels.common.error.ErrorCode;
import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.exception.RetinaException;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.Column;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.utils.Bitmap;
import io.pixelsdb.pixels.core.RetinaProto;
import io.pixelsdb.pixels.core.RetinaServiceGrpc;
import io.pixelsdb.pixels.core.vector.BinaryColumnVector;
import io.pixelsdb.pixels.core.vector.LongColumnVector;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;

import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;


/**
 * client interacting with retina
 */
public class RetinaService {
    private final ManagedChannel channel;
    private final RetinaServiceGrpc.RetinaServiceBlockingStub stub;

    private final MemoryMappedFile queryMem;
    private final MemoryMappedFile versionMem;
    HashMap<String, TypeDescription> schemas;
    MetadataService metadataService;


    public RetinaService(String host, int port, MetadataService metadataService) {
        assert (host != null);
        assert (port > 0 && port <= 65535);
        this.channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext().build();
        this.stub = RetinaServiceGrpc.newBlockingStub(channel);
        this.metadataService = metadataService;
        try {
            this.queryMem = new MemoryMappedFile("/dev/shm/query", 200 * 1024);
            this.versionMem = new MemoryMappedFile("/dev/shm/version", 100 * 1024);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void shutdown() throws InterruptedException {
        this.channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public VectorizedRowBatch queryRecords(String schemaName, String tableName, int rgid, long timestamp) throws RetinaException {
        String token = UUID.randomUUID().toString();
        RetinaProto.RequestHeader header = RetinaProto.RequestHeader.newBuilder().setToken(token).build();

        RetinaProto.QueryRecordsRequest request = RetinaProto.QueryRecordsRequest.newBuilder()
                .setHeader(header)
                .setSchemaName(schemaName)
                .setTableName(tableName)
                .setRgid(rgid)
                .setTimestamp(timestamp)
                .build();
        try {
            RetinaProto.QueryRecordsResponse response = this.stub.queryRecords(request);
            if (response.getHeader().getErrorCode() != ErrorCode.SUCCESS) {
                throw new RetinaException("failed to queryRecords, error code=" + response.getHeader().getErrorCode());
            }
            long pos = response.getPos();
            VectorizedRowBatch res = readRowBatch(pos, schemaName, tableName);

            RetinaProto.QueryAck ack = RetinaProto.QueryAck.newBuilder()
                    .setHeader(header)
                    .setPos(pos)
                    .build();
            RetinaProto.ResponseHeader response_ = this.stub.finishRecords(ack);
            if (response_.getErrorCode() != ErrorCode.SUCCESS) {
                throw new RetinaException("failed to ack queryRecords, error code=" + response.getHeader().getErrorCode());
            }

            return res;
        } catch (Exception e) {
            throw new RetinaException("failed to queryRecords", e);
        }
    }

    public Bitmap getVisibility(String schemaName, String tableName, int rgid, long timestamp) throws RetinaException {
        String token = UUID.randomUUID().toString();
        RetinaProto.RequestHeader header = RetinaProto.RequestHeader.newBuilder().setToken(token).build();

        RetinaProto.QueryVisibilityRequest request = RetinaProto.QueryVisibilityRequest.newBuilder()
                .setHeader(header)
                .setSchemaName(schemaName)
                .setTableName(tableName)
                .setRgid(rgid)
                .setTimestamp(timestamp)
                .build();
        try {
            RetinaProto.QueryVisibilityResponse response = this.stub.queryVisibility(request);
            if (response.getHeader().getErrorCode() != ErrorCode.SUCCESS) {
                throw new RetinaException("failed to queryRecords, error code=" + response.getHeader().getErrorCode());
            }
            long pos = response.getPos();
            Bitmap res = readBitmap(pos);
            RetinaProto.QueryAck ack = RetinaProto.QueryAck.newBuilder()
                    .setHeader(header)
                    .setPos(pos)
                    .build();
            RetinaProto.ResponseHeader response_ = this.stub.finishVisibility(ack);
            if (response_.getErrorCode() != ErrorCode.SUCCESS) {
                throw new RetinaException("failed to ack queryVisibility, error code=" + response.getHeader().getErrorCode());
            }

            return res;
        } catch (Exception e) {
            throw new RetinaException("failed to queryRecords", e);
        }
    }

    /**
     * Read {@link VectorizedRowBatch} from shared memory written by retina
     * <br>
     * TODO: unify with {@link io.pixelsdb.pixels.retina.RetinaWriter#readAndWrite}
     */
    VectorizedRowBatch readRowBatch(long pos, String schemaName, String tableName) throws MetadataException {
        TypeDescription schema = getSchema(schemaName, tableName);
        VectorizedRowBatch res = schema.createRowBatch();
        // Result format: [col number (int32)] [row number (int32)] [cols]
        // Long column:
        // [value type (int16)]
        // [data length (unit: char) (int32)] [data]
        // Binary column:
        // [value type (int16)]
        // [offset/length array length (unit: char) (int32)]
        // [offset array] [length array]
        // [data length (unit: char) (int32)] [data]
        int colNum = queryMem.getInt(pos);
        pos += Integer.BYTES;
        int rowNum = queryMem.getInt(pos);
        pos += Integer.BYTES;

        for (int i = 0; i < colNum; i++) {
            short type = queryMem.getShort(pos);
            pos += Short.BYTES;

            ValueType valueType = ValueType.fromShort(type);
            switch (Objects.requireNonNull(valueType)) {
                case LONG:
                    int length = queryMem.getInt(pos);
                    pos += Integer.BYTES;
                    LongColumnVector longCol = (LongColumnVector) res.cols[i];
                    for (int j = 0; j < rowNum; j++) {
                        longCol.vector[j] = queryMem.getLong(pos);
                        pos += Long.BYTES;
                    }

                    break;

                case BYTES:
                    int lenLength = queryMem.getInt(pos);
                    pos += Integer.BYTES;
                    BinaryColumnVector strCol = (BinaryColumnVector) res.cols[i];

                    for (int j = 0; j < rowNum; j++) {
                        strCol.start[j] = queryMem.getInt(pos);
                        pos += Integer.BYTES;
                    }

                    for (int j = 0; j < rowNum; j++) {
                        strCol.lens[j] = queryMem.getInt(pos);
                        pos += Integer.BYTES;
                    }

                    int dataLength = queryMem.getInt(pos);
                    pos += Integer.BYTES;

                    queryMem.getBytes(pos, strCol.buffer, 0, dataLength);
                    pos += dataLength;

                    for (int j = 0; j < rowNum; j++) {
                        strCol.vector[j] = strCol.buffer;
                    }

                    break;
            }
        }

        return res;
    }

    /**
     * Read {@link Bitmap} from shared memory written by retina
     */
    Bitmap readBitmap(long pos) {
        // result format:
        // [uint64_t array length (unit: char)]
        // [uint64_t array]
        long length = versionMem.getLong(pos);
        Bitmap res = new Bitmap((int) (length * 64), false);
        pos += Long.BYTES;
        for (int i = 0; i < length; i++) {
            long value = versionMem.getLong(pos);
            pos += Long.BYTES;
            res.setWord(i, value);
        }

        return res;
    }

    /**
     * copied from {@link io.pixelsdb.pixels.retina.RetinaWriter#getSchema}
     */
    TypeDescription getSchema(String schemaName, String tableName) throws MetadataException {
        String key = schemaName + ":" + tableName;
        TypeDescription schema = schemas.getOrDefault(key, null);
        if (schema == null) {
            List<Column> columns = metadataService.getColumns(schemaName, tableName);
            StringBuilder builder = new StringBuilder();
            builder.append("struct<");
            String prefix = "struct<";
            for (Column col : columns) {
                builder.append(prefix);
                builder.append(col.getName());
                builder.append(":");
                builder.append(col.getType());
                prefix = ",";
            }
            builder.append(">");
            String schemaStr = builder.toString();
            schema = TypeDescription.fromString(schemaStr);
            schemas.put(key, schema);
        }
        return schema;
    }

    /**
     * copied from {@link io.pixelsdb.pixels.retina.RetinaWriter.ValueType}
     */
    enum ValueType {
        NONE,  // Useful when the type of column vector has not be determined yet.
        LONG,
        DOUBLE,
        BYTES,
        DECIMAL,
        TIMESTAMP,
        INTERVAL_DAY_TIME,
        STRUCT,
        LIST,
        MAP,
        UNION;

        public static ValueType fromShort(short x) {
            switch (x) {
                case 0:
                    return NONE;
                case 1:
                    return LONG;
                case 3:
                    return BYTES;
            }
            return null;
        }
    }
}
