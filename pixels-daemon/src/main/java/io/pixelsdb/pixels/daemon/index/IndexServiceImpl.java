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
import io.pixelsdb.pixels.common.error.ErrorCode;
import io.pixelsdb.pixels.common.exception.MainIndexException;
import io.pixelsdb.pixels.common.exception.SinglePointIndexException;
import io.pixelsdb.pixels.common.index.SinglePointIndex;
import io.pixelsdb.pixels.common.index.MainIndex;
import io.pixelsdb.pixels.index.IndexProto;
import io.pixelsdb.pixels.index.IndexServiceGrpc;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author hank, Rolland1944
 * @create 2025-02-19
 */
public class IndexServiceImpl extends IndexServiceGrpc.IndexServiceImplBase
{
    private static final Logger logger = LogManager.getLogger(IndexServiceImpl.class);
    private final SinglePointIndex singlePointIndex;
    private final MainIndex mainIndex;

    public IndexServiceImpl(SinglePointIndex singlePointIndex, MainIndex mainIndex)
    {
        this.singlePointIndex = singlePointIndex;
        this.mainIndex = mainIndex;
    }

    @Override
    public void allocateRowIdBatch(IndexProto.AllocateRowIdBatchRequest request,
                                   StreamObserver<IndexProto.AllocateRowIdBatchResponse> responseObserver)
    {
        long tableId = request.getTableId();
        int numRowIds = request.getNumRowIds();
        IndexProto.RowIdBatch rowIdBatch = mainIndex.allocateRowIdBatch(tableId, numRowIds);
        IndexProto.AllocateRowIdBatchResponse;
        if(RowIdBatch != null)
        {
            response = AllocateRowIdBatchResponse.newBuilder()
            .setErrorCode(ErrorCode.SUCCESS)
            .setRowIdBatch(rowIdBatch)
            .build();
        }
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void lookupUniqueIndex(IndexProto.LookupUniqueIndexRequest request,
                                  StreamObserver<IndexProto.LookupUniqueIndexResponse> responseObserver)
    {
        // Get IndexKey from request
        IndexProto.IndexKey key = request.getIndexKey();

        // Call SinglePointIndex's getUniqueRowId method
        long rowId = singlePointIndex.getUniqueRowId(key);

        // Call MainIndex's getLocation method to convert rowId to RowLocation
        IndexProto.RowLocation rowLocation = mainIndex.getLocation(rowId);

        // Create gRPC response
        IndexProto.LookupUniqueIndexResponse response;
        if (rowLocation != null)
        {
            response = IndexProto.LookupUniqueIndexResponse.newBuilder()
                    .setRowLocation(rowLocation)
                    .build();
        }
        else
        {
            // If not found, return empty RowLocation
            response = IndexProto.LookupUniqueIndexResponse.newBuilder()
                    .setRowLocation(IndexProto.RowLocation.getDefaultInstance())
                    .build();
        }

        // Send response
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void lookupNonUniqueIndex(IndexProto.LookupNonUniqueIndexRequest request,
                                     StreamObserver<IndexProto.LookupNonUniqueIndexResponse> responseObserver)
    {
        // Get IndexKey from request
        IndexProto.IndexKey key = request.getIndexKey();

        // Call SinglePointIndex's getRowIds method
        long[] rowIds = singlePointIndex.getRowIds(key);

        // Convert rowIds to list of RowLocations
        List<IndexProto.RowLocation> rowLocations = new ArrayList<>();
        for (long rowId : rowIds)
        {
            IndexProto.RowLocation rowLocation = mainIndex.getLocation(rowId);
            if (rowLocation != null)
            {
                rowLocations.add(rowLocation);
            }
        }

        // Create gRPC response
        IndexProto.LookupNonUniqueIndexResponse response = IndexProto.LookupNonUniqueIndexResponse.newBuilder()
                .addAllRowLocation(rowLocations)
                .build();

        // Send response
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void putPrimaryIndexEntry(IndexProto.PutPrimaryIndexEntryRequest request,
                              StreamObserver<IndexProto.PutPrimaryIndexEntryResponse> responseObserver)
    {
        // Get IndexEntry from request
        IndexProto.PrimaryIndexEntry entry = request.getIndexEntry();
        // Create gRPC builder
        IndexProto.PutPrimaryIndexEntryResponse.Builder builder = IndexProto.PutPrimaryIndexEntryResponse.newBuilder();
        try
        {
            // Call SinglePointIndex's putEntry method
            boolean success = singlePointIndex.putPrimaryEntry(
                    new SinglePointIndex.Entry(entry.getIndexKey(), entry.getTableRowId(), true, entry.getRowLocation()));
            // Create gRPC response
            builder.setErrorCode(ErrorCode.SUCCESS);
        }
        catch (MainIndexException e)
        {
            builder.setErrorCode(ErrorCode.INDEX_PUT_MAIN_INDEX_FAIL);
        }
        catch (SinglePointIndexException e)
        {
            builder.setErrorCode(ErrorCode.INDEX_PUT_SINGLE_POINT_INDEX_FAIL);
        }
        // Send response
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void putSecondaryIndexEntry(IndexProto.PutSecondaryIndexEntryRequest request,
                                       StreamObserver<IndexProto.PutSecondaryIndexEntryResponse> responseObserver)
    {
        // Get IndexEntry from request
        IndexProto.SecondaryIndexEntry entry = request.getIndexEntry();
        // Create gRPC builder
        IndexProto.PutSecondaryIndexEntryResponse.Builder builder = IndexProto.PutSecondaryIndexEntryResponse.newBuilder();
        try
        {
            // Call SinglePointIndex's putEntry method
            boolean success = singlePointIndex.putSecondaryEntry(
                    new SinglePointIndex.Entry(entry.getIndexKey(), entry.getTableRowId(), true, entry.getRowLocation()));
            // Create gRPC response
            builder.setErrorCode(ErrorCode.SUCCESS);
        }
        catch (SinglePointIndexException e)
        {
            builder.setErrorCode(ErrorCode.INDEX_PUT_SINGLE_POINT_INDEX_FAIL);
        }
        // Send response
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void deletePrimaryIndexEntry(IndexProto.DeletePrimaryIndexEntryRequest request,
                                 StreamObserver<IndexProto.DeletePrimaryIndexEntryResponse> responseObserver)
    {
        // Get IndexKey from request
        IndexProto.IndexKey key = request.getIndexKey();
        // Create gRPC builder
        IndexProto.DeletePrimaryIndexEntryResponse.Builder builder = IndexProto.DeletePrimaryIndexEntryResponse.newBuilder();
        try
        {
            // Call SinglePointIndex's deleteEntry method
            boolean success = singlePointIndex.deletePrimaryEntry(key);
            builder.setErrorCode(ErrorCode.SUCCESS);
        }
        catch (MainIndexException e)
        {
            builder.setErrorCode(ErrorCode.INDEX_DELETE_MAIN_INDEX_FAIL);
        }
        catch (SinglePointIndexException e)
        {
            builder.setErrorCode(ErrorCode.INDEX_DELETE_SINGLE_POINT_INDEX_FAIL);
        }
        // Send response
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

        @Override
    public void deleteSecondaryIndexEntry(IndexProto.DeleteSecondaryIndexEntryRequest request,
                                 StreamObserver<IndexProto.DeleteSecondaryIndexEntryResponse> responseObserver)
    {
        // Get IndexKey from request
        IndexProto.IndexKey key = request.getIndexKey();
        // Create gRPC builder
        IndexProto.DeleteSecondaryIndexEntryResponse.Builder builder = IndexProto.DeleteSecondaryIndexEntryResponse.newBuilder();
        try
        {
            // Call SinglePointIndex's deleteEntry method
            boolean success = singlePointIndex.deleteSecondaryEntry(key);
            builder.setErrorCode(ErrorCode.SUCCESS);
        }
        catch (SinglePointIndexException e)
        {
            builder.setErrorCode(ErrorCode.INDEX_DELETE_SINGLE_POINT_INDEX_FAIL);
        }
        // Send response
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void putPrimaryIndexEntries(IndexProto.PutPrimaryIndexEntriesRequest request,
                                StreamObserver<IndexProto.PutPrimaryIndexEntriesResponse> responseObserver)
    {
        // Get list of IndexEntries from request
        List<SinglePointIndex.Entry> entries = request.getIndexEntriesList().stream()
                .map(entry -> new SinglePointIndex.Entry(
                        entry.getIndexKey(), entry.getTableRowId(), true, entry.getRowLocation()))
                .collect(Collectors.toList());
        // Create gRPC builder
        IndexProto.PutPrimaryIndexEntriesResponse.Builder builder  = IndexProto.PutPrimaryIndexEntriesResponse.newBuilder();
        try
        {
            // Call SinglePointIndex's putEntries method
            boolean success = singlePointIndex.putPrimaryEntries(entries);
            builder.setErrorCode(ErrorCode.SUCCESS);
        }
        catch (MainIndexException e)
        {
            builder.setErrorCode(ErrorCode.INDEX_PUT_MAIN_INDEX_FAIL);
        }
        catch (SinglePointIndexException e)
        {
            builder.setErrorCode(ErrorCode.INDEX_PUT_SINGLE_POINT_INDEX_FAIL);
        }
        // Send response
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void putSecondaryIndexEntries(IndexProto.PutSecondaryIndexEntriesRequest request,
                                         StreamObserver<IndexProto.PutSecondaryIndexEntriesResponse> responseObserver)
    {
        // Get list of IndexEntries from request
        List<SinglePointIndex.Entry> entries = request.getIndexEntriesList().stream()
                .map(entry -> new SinglePointIndex.Entry(
                        entry.getIndexKey(), entry.getTableRowId(), true, entry.getRowLocation()))
                .collect(Collectors.toList());
        // Create gRPC builder
        IndexProto.PutSecondaryIndexEntriesResponse.Builder builder  = IndexProto.PutSecondaryIndexEntriesResponse.newBuilder();
        try
        {
            // Call SinglePointIndex's putEntries method
            boolean success = singlePointIndex.putSecondaryEntries(entries);
            builder.setErrorCode(ErrorCode.SUCCESS);
        }
        catch (SinglePointIndexException e)
        {
            builder.setErrorCode(ErrorCode.INDEX_PUT_SINGLE_POINT_INDEX_FAIL);
        }
        // Send response
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void deletePrimaryIndexEntries(IndexProto.DeletePrimaryIndexEntriesRequest request,
                                   StreamObserver<IndexProto.DeletePrimaryIndexEntriesResponse> responseObserver)
    {
        // Get list of IndexKeys from request
        List<IndexProto.IndexKey> keys = request.getIndexKeysList();
        // Create gRPC builder
        IndexProto.DeletePrimaryIndexEntriesResponse.Builder builder = IndexProto.DeletePrimaryIndexEntriesResponse.newBuilder();
        try
        {
            // Call SinglePointIndex's deleteEntries method
            boolean success = singlePointIndex.deletePrimaryEntries(keys);
            builder.setErrorCode(ErrorCode.SUCCESS);
        }
        catch (MainIndexException e)
        {
            builder.setErrorCode(ErrorCode.INDEX_DELETE_MAIN_INDEX_FAIL);
        }
        catch (SinglePointIndexException e)
        {
            builder.setErrorCode(ErrorCode.INDEX_DELETE_SINGLE_POINT_INDEX_FAIL);
        }
        // Send response
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

        @Override
    public void deleteSecondaryIndexEntries(IndexProto.DeleteSecondaryIndexEntriesRequest request,
                                   StreamObserver<IndexProto.DeleteSecondaryIndexEntriesResponse> responseObserver)
    {
        // Get list of IndexKeys from request
        List<IndexProto.IndexKey> keys = request.getIndexKeysList();
        // Create gRPC builder
        IndexProto.DeleteSecondaryIndexEntriesResponse.Builder builder = IndexProto.DeleteSecondaryIndexEntriesResponse.newBuilder();
        try
        {
            // Call SinglePointIndex's deleteEntries method
            boolean success = singlePointIndex.deleteSecondaryEntries(keys);
            builder.setErrorCode(ErrorCode.SUCCESS);
        }
        catch (SinglePointIndexException e)
        {
            builder.setErrorCode(ErrorCode.INDEX_DELETE_SINGLE_POINT_INDEX_FAIL);
        }
        // Send response
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }
}
