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
 * @author hank, Rolland1944
 * @create 2025-02-19
 */
public class IndexServiceImpl extends IndexServiceGrpc.IndexServiceImplBase
{
    private static final Logger logger = LogManager.getLogger(IndexServiceImpl.class);
    private final SecondaryIndex secondaryIndex;
    private final MainIndex mainIndex;

    public IndexServiceImpl(SecondaryIndex secondaryIndex, MainIndex mainIndex)
    {
        this.secondaryIndex = secondaryIndex;
        this.mainIndex = mainIndex;
    }

    @Override
    public void lookupUniqueIndex(IndexProto.LookupUniqueIndexRequest request,
                                  StreamObserver<IndexProto.LookupUniqueIndexResponse> responseObserver)
    {
        // Get IndexKey from request
        IndexProto.IndexKey key = request.getIndexKey();

        // Call SecondaryIndex's getUniqueRowId method
        long rowId = secondaryIndex.getUniqueRowId(key);

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

        // Call SecondaryIndex's getRowIds method
        long[] rowIds = secondaryIndex.getRowIds(key);

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
    public void putIndexEntry(IndexProto.PutIndexEntryRequest request,
                              StreamObserver<IndexProto.PutIndexEntryResponse> responseObserver)
    {
        // Get IndexEntry from request
        IndexProto.IndexEntry entry = request.getIndexEntry();

        // Call SecondaryIndex's putEntry method
        long rowId = secondaryIndex.putEntry(new SecondaryIndex.Entry(entry.getIndexKey(), 0, entry.getUnique(), entry.getRowLocation()));

        // Create gRPC response
        IndexProto.PutIndexEntryResponse response = IndexProto.PutIndexEntryResponse.newBuilder()
                .setRowId(rowId)
                .build();

        // Send response
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void deleteIndexEntry(IndexProto.DeleteIndexEntryRequest request,
                                 StreamObserver<IndexProto.DeleteIndexEntryResponse> responseObserver)
    {
        // Get IndexKey from request
        IndexProto.IndexKey key = request.getIndexKey();

        // Call SecondaryIndex's deleteEntry method
        boolean success = secondaryIndex.deleteEntry(key);

        // Create gRPC response
        IndexProto.DeleteIndexEntryResponse response = IndexProto.DeleteIndexEntryResponse.newBuilder()
                .setErrorCode(success ? 0 : 1)
                .build();

        // Send response
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void putIndexEntries(IndexProto.PutIndexEntriesRequest request,
                                StreamObserver<IndexProto.PutIndexEntriesResponse> responseObserver)
    {
        // Get list of IndexEntries from request
        List<SecondaryIndex.Entry> entries = request.getIndexEntriesList().stream()
                .map(entry -> new SecondaryIndex.Entry(entry.getIndexKey(), 0, entry.getUnique(), entry.getRowLocation()))
                .collect(Collectors.toList());

        // Call SecondaryIndex's putEntries method
        List<Long> rowIds = secondaryIndex.putEntries(entries);

        // Create gRPC response
        IndexProto.PutIndexEntriesResponse response = IndexProto.PutIndexEntriesResponse.newBuilder()
                .addAllRowIds(rowIds)
                .build();

        // Send response
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void deleteIndexEntries(IndexProto.DeleteIndexEntriesRequest request,
                                   StreamObserver<IndexProto.DeleteIndexEntriesResponse> responseObserver)
    {
        // Get list of IndexKeys from request
        List<IndexProto.IndexKey> keys = request.getIndexKeysList();

        // Call SecondaryIndex's deleteEntries method
        boolean success = secondaryIndex.deleteEntries(keys);

        // Create gRPC response
        IndexProto.DeleteIndexEntriesResponse response = IndexProto.DeleteIndexEntriesResponse.newBuilder()
                .setErrorCode(success ? 0 : 1)
                .build();

        // Send response
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
