/*
 * Copyright 2025 PixelsDB.
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
package io.pixelsdb.pixels.daemon.index;

import io.grpc.stub.StreamObserver;
import io.pixelsdb.pixels.common.index.SecondaryIndex;
import io.pixelsdb.pixels.common.index.MainIndex;
import io.pixelsdb.pixels.index.IndexProto;
import io.pixelsdb.pixels.index.IndexServiceGrpc;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author hank
 * @create 2025-02-19
 */
public class IndexServiceImpl extends IndexServiceGrpc.IndexServiceImplBase
{
    // TODO: implement
    private static final Logger logger = LogManager.getLogger(IndexServiceImpl.class);
    private final SecondaryIndex secondaryIndex;
    private final MainIndex mainIndex;

//    public IndexServiceImpl() {}
    public IndexServiceImpl(SecondaryIndex secondaryIndex, MainIndex mainIndex) {
        this.secondaryIndex = secondaryIndex;
        this.mainIndex = mainIndex;
    }
    @Override
    public void lookupUniqueIndex(IndexProto.LookupUniqueIndexRequest request,
                                  StreamObserver<IndexProto.LookupUniqueIndexResponse> responseObserver)
    {
        // 从请求中获取 IndexKey
        IndexProto.IndexKey key = request.getIndexKey();

        // 调用 SecondaryIndex 的 getUniqueRowId 方法
        long rowId = secondaryIndex.getUniqueRowId(key);

        // 调用 MainIndex 的 getLocation 方法，将 rowId 转换为 RowLocation
        IndexProto.RowLocation rowLocation = mainIndex.getLocation(rowId);

        // 创建 gRPC 响应
        IndexProto.LookupUniqueIndexResponse response;
        if (rowLocation != null) {
            response = IndexProto.LookupUniqueIndexResponse.newBuilder()
                    .setRowLocation(rowLocation)
                    .build();
        } else {
            // 如果未找到，返回空的 RowLocation
            response = IndexProto.LookupUniqueIndexResponse.newBuilder()
                    .setRowLocation(IndexProto.RowLocation.getDefaultInstance())
                    .build();
        }

        // 发送响应
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void lookupNonUniqueIndex(IndexProto.LookupNonUniqueIndexRequest request,
                                     StreamObserver<IndexProto.LookupNonUniqueIndexResponse> responseObserver)
    {
        // 从请求中获取 IndexKey
        IndexProto.IndexKey key = request.getIndexKey();

        // 调用 SecondaryIndex 的 getRowIds 方法
        long[] rowIds = secondaryIndex.getRowIds(key);

        // 将 rowIds 转换为 RowLocation 列表
        List<IndexProto.RowLocation> rowLocations = new ArrayList<>();
        for (long rowId : rowIds) {
            IndexProto.RowLocation rowLocation = mainIndex.getLocation(rowId);
            if (rowLocation != null) {
                rowLocations.add(rowLocation);
            }
        }

        // 创建 gRPC 响应
        IndexProto.LookupNonUniqueIndexResponse response = IndexProto.LookupNonUniqueIndexResponse.newBuilder()
                .addAllRowLocation(rowLocations)
                .build();

        // 发送响应
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void putIndexEntry(IndexProto.PutIndexEntryRequest request,
                              StreamObserver<IndexProto.PutIndexEntryResponse> responseObserver)
    {
        // 从请求中获取 IndexEntry
        IndexProto.IndexEntry entry = request.getIndexEntry();

        // 调用 SecondaryIndex 的 putEntry 方法
        boolean success = secondaryIndex.putEntry(new SecondaryIndex.Entry(entry.getIndexKey(), 0, entry.getUnique()));

        // 创建 gRPC 响应
        IndexProto.PutIndexEntryResponse response = IndexProto.PutIndexEntryResponse.newBuilder()
                .setErrorCode(success ? 0 : 1)
                .build();

        // 发送响应
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void deleteIndexEntry(IndexProto.DeleteIndexEntryRequest request,
                                 StreamObserver<IndexProto.DeleteIndexEntryResponse> responseObserver)
    {
        // 从请求中获取 IndexKey
        IndexProto.IndexKey key = request.getIndexKey();

        // 调用 SecondaryIndex 的 deleteEntry 方法
        boolean success = secondaryIndex.deleteEntry(key);

        // 创建 gRPC 响应
        IndexProto.DeleteIndexEntryResponse response = IndexProto.DeleteIndexEntryResponse.newBuilder()
                .setErrorCode(success ? 0 : 1)
                .build();

        // 发送响应
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void putIndexEntries(IndexProto.PutIndexEntriesRequest request,
                                StreamObserver<IndexProto.PutIndexEntriesResponse> responseObserver)
    {
        // 从请求中获取 IndexEntry 列表
        List<SecondaryIndex.Entry> entries = request.getIndexEntriesList().stream()
                .map(entry -> new SecondaryIndex.Entry(entry.getIndexKey(), 0, entry.getUnique()))
                .collect(Collectors.toList());

        // 调用 SecondaryIndex 的 putEntries 方法
        boolean success = secondaryIndex.putEntries(entries);

        // 创建 gRPC 响应
        IndexProto.PutIndexEntriesResponse response = IndexProto.PutIndexEntriesResponse.newBuilder()
                .setErrorCode(success ? 0 : 1)
                .build();

        // 发送响应
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void deleteIndexEntries(IndexProto.DeleteIndexEntriesRequest request,
                                   StreamObserver<IndexProto.DeleteIndexEntriesResponse> responseObserver)
    {
        // 从请求中获取 IndexKey 列表
        List<IndexProto.IndexKey> keys = request.getIndexKeysList();

        // 调用 SecondaryIndex 的 deleteEntries 方法
        boolean success = secondaryIndex.deleteEntries(keys);

        // 创建 gRPC 响应
        IndexProto.DeleteIndexEntriesResponse response = IndexProto.DeleteIndexEntriesResponse.newBuilder()
                .setErrorCode(success ? 0 : 1)
                .build();

        // 发送响应
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
