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
import io.pixelsdb.pixels.common.exception.RowIdException;
import io.pixelsdb.pixels.common.exception.SinglePointIndexException;
import io.pixelsdb.pixels.common.index.MainIndex;
import io.pixelsdb.pixels.common.index.SinglePointIndex;
import io.pixelsdb.pixels.index.IndexProto;
import io.pixelsdb.pixels.index.IndexServiceGrpc;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

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
        IndexProto.RowIdBatch rowIdBatch = null;
        IndexProto.AllocateRowIdBatchResponse.Builder response = IndexProto.AllocateRowIdBatchResponse.newBuilder();
        try
        {
            rowIdBatch = mainIndex.allocateRowIdBatch(tableId, numRowIds);
        } catch (RowIdException e)
        {
            logger.error("failed to allocate row ids", e);
            response.setErrorCode(ErrorCode.INDEX_GET_ROW_ID_FAIL);
        }
        if(rowIdBatch != null)
        {
            response.setErrorCode(ErrorCode.SUCCESS).setRowIdBatch(rowIdBatch);
        }
        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    @Override
    public void lookupUniqueIndex(IndexProto.LookupUniqueIndexRequest request,
                                  StreamObserver<IndexProto.LookupUniqueIndexResponse> responseObserver)
    {
        IndexProto.IndexKey key = request.getIndexKey();
        IndexProto.LookupUniqueIndexResponse.Builder builder = IndexProto.LookupUniqueIndexResponse.newBuilder();
        try
        {
            long rowId = singlePointIndex.getUniqueRowId(key);
            IndexProto.RowLocation rowLocation = mainIndex.getLocation(rowId);

            if (rowLocation != null)
            {
                builder.setRowLocation(rowLocation);
            }
            else
            {
                // If not found, return empty RowLocation
                builder.setErrorCode(ErrorCode.SUCCESS).setRowLocation(IndexProto.RowLocation.getDefaultInstance());
            }
        }
        catch (SinglePointIndexException e)
        {
            builder.setErrorCode(ErrorCode.INDEX_GET_ROW_ID_FAIL);
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void lookupNonUniqueIndex(IndexProto.LookupNonUniqueIndexRequest request,
                                     StreamObserver<IndexProto.LookupNonUniqueIndexResponse> responseObserver)
    {
        IndexProto.IndexKey key = request.getIndexKey();
        IndexProto.LookupNonUniqueIndexResponse.Builder builder = IndexProto.LookupNonUniqueIndexResponse.newBuilder();
        try
        {
            List<Long> rowIds = singlePointIndex.getNonUniqueRowIds(key);
            List<IndexProto.RowLocation> rowLocations = new ArrayList<>();
            for (long rowId : rowIds)
            {
                IndexProto.RowLocation rowLocation = mainIndex.getLocation(rowId);
                if (rowLocation != null)
                {
                    rowLocations.add(rowLocation);
                }
            }
            builder.setErrorCode(ErrorCode.SUCCESS).addAllRowLocations(rowLocations);
        }
        catch (SinglePointIndexException e)
        {
            builder.setErrorCode(ErrorCode.INDEX_GET_ROW_ID_FAIL);
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void putPrimaryIndexEntry(IndexProto.PutPrimaryIndexEntryRequest request,
                              StreamObserver<IndexProto.PutPrimaryIndexEntryResponse> responseObserver)
    {
        IndexProto.PrimaryIndexEntry entry = request.getIndexEntry();
        IndexProto.PutPrimaryIndexEntryResponse.Builder builder = IndexProto.PutPrimaryIndexEntryResponse.newBuilder();
        try
        {
            boolean success = singlePointIndex.putEntry(entry.getIndexKey(), entry.getTableRowId(), true);
            // TODO: put main index
            if (success)
            {
                builder.setErrorCode(ErrorCode.SUCCESS);
            }
            else
            {
                builder.setErrorCode(ErrorCode.INDEX_PUT_SINGLE_POINT_INDEX_FAIL);
            }
        }
        //catch (MainIndexException e)
        //{
        //    builder.setErrorCode(ErrorCode.INDEX_PUT_MAIN_INDEX_FAIL);
        //}
        catch (SinglePointIndexException e)
        {
            builder.setErrorCode(ErrorCode.INDEX_PUT_SINGLE_POINT_INDEX_FAIL);
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void putPrimaryIndexEntries(IndexProto.PutPrimaryIndexEntriesRequest request,
                                       StreamObserver<IndexProto.PutPrimaryIndexEntriesResponse> responseObserver)
    {
        List<IndexProto.PrimaryIndexEntry> entries = request.getIndexEntriesList();
        IndexProto.PutPrimaryIndexEntriesResponse.Builder builder = IndexProto.PutPrimaryIndexEntriesResponse.newBuilder();
        try
        {
            boolean success = singlePointIndex.putPrimaryEntries(entries);
            // TODO: put main index
            if (success)
            {
                builder.setErrorCode(ErrorCode.SUCCESS);
            }
            else
            {
                builder.setErrorCode(ErrorCode.INDEX_PUT_SINGLE_POINT_INDEX_FAIL);
            }
        }
        //catch (MainIndexException e)
        //{
        //    builder.setErrorCode(ErrorCode.INDEX_PUT_MAIN_INDEX_FAIL);
        //}
        catch (SinglePointIndexException e)
        {
            builder.setErrorCode(ErrorCode.INDEX_PUT_SINGLE_POINT_INDEX_FAIL);
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void putSecondaryIndexEntry(IndexProto.PutSecondaryIndexEntryRequest request,
                                       StreamObserver<IndexProto.PutSecondaryIndexEntryResponse> responseObserver)
    {
        IndexProto.SecondaryIndexEntry entry = request.getIndexEntry();
        IndexProto.PutSecondaryIndexEntryResponse.Builder builder = IndexProto.PutSecondaryIndexEntryResponse.newBuilder();
        try
        {
            boolean success = singlePointIndex.putEntry(entry.getIndexKey(), entry.getTableRowId(), entry.getUnique());
            if (success)
            {
                builder.setErrorCode(ErrorCode.SUCCESS);
            }
            else
            {
                builder.setErrorCode(ErrorCode.INDEX_PUT_SINGLE_POINT_INDEX_FAIL);
            }
        }
        catch (SinglePointIndexException e)
        {
            builder.setErrorCode(ErrorCode.INDEX_PUT_SINGLE_POINT_INDEX_FAIL);
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void putSecondaryIndexEntries(IndexProto.PutSecondaryIndexEntriesRequest request,
                                         StreamObserver<IndexProto.PutSecondaryIndexEntriesResponse> responseObserver)
    {
        List<IndexProto.SecondaryIndexEntry> entries = request.getIndexEntriesList();
        IndexProto.PutSecondaryIndexEntriesResponse.Builder builder = IndexProto.PutSecondaryIndexEntriesResponse.newBuilder();
        try
        {
            boolean success = singlePointIndex.putSecondaryEntries(entries);
            if (success)
            {
                builder.setErrorCode(ErrorCode.SUCCESS);
            }
            else
            {
                builder.setErrorCode(ErrorCode.INDEX_PUT_SINGLE_POINT_INDEX_FAIL);
            }
        }
        catch (SinglePointIndexException e)
        {
            builder.setErrorCode(ErrorCode.INDEX_PUT_SINGLE_POINT_INDEX_FAIL);
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void deletePrimaryIndexEntry(IndexProto.DeletePrimaryIndexEntryRequest request,
                                 StreamObserver<IndexProto.DeletePrimaryIndexEntryResponse> responseObserver)
    {
        IndexProto.IndexKey key = request.getIndexKey();
        IndexProto.DeletePrimaryIndexEntryResponse.Builder builder = IndexProto.DeletePrimaryIndexEntryResponse.newBuilder();
        try
        {
            long rowId = singlePointIndex.deleteEntry(key);
            // TODO: delete main index
            builder.setErrorCode(ErrorCode.SUCCESS);
        }
        //catch (MainIndexException e)
        //{
        //    builder.setErrorCode(ErrorCode.INDEX_DELETE_MAIN_INDEX_FAIL);
        //}
        catch (SinglePointIndexException e)
        {
            builder.setErrorCode(ErrorCode.INDEX_DELETE_SINGLE_POINT_INDEX_FAIL);
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void deletePrimaryIndexEntries(IndexProto.DeletePrimaryIndexEntriesRequest request,
                                          StreamObserver<IndexProto.DeletePrimaryIndexEntriesResponse> responseObserver)
    {
        List<IndexProto.IndexKey> keys = request.getIndexKeysList();
        IndexProto.DeletePrimaryIndexEntriesResponse.Builder builder = IndexProto.DeletePrimaryIndexEntriesResponse.newBuilder();
        try
        {
            List<Long> rowLocations = singlePointIndex.deleteEntries(keys);
            // TODO: delete main index
            builder.setErrorCode(ErrorCode.SUCCESS);
        }
        //catch (MainIndexException e)
        //{
        //    builder.setErrorCode(ErrorCode.INDEX_DELETE_MAIN_INDEX_FAIL);
        //}
        catch (SinglePointIndexException e)
        {
            builder.setErrorCode(ErrorCode.INDEX_DELETE_SINGLE_POINT_INDEX_FAIL);
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void deleteSecondaryIndexEntry(IndexProto.DeleteSecondaryIndexEntryRequest request,
                                 StreamObserver<IndexProto.DeleteSecondaryIndexEntryResponse> responseObserver)
    {
        IndexProto.IndexKey key = request.getIndexKey();
        IndexProto.DeleteSecondaryIndexEntryResponse.Builder builder = IndexProto.DeleteSecondaryIndexEntryResponse.newBuilder();
        try
        {
            long rowId = singlePointIndex.deleteEntry(key);
            builder.setErrorCode(ErrorCode.SUCCESS).setTableRowId(rowId);
        }
        catch (SinglePointIndexException e)
        {
            builder.setErrorCode(ErrorCode.INDEX_DELETE_SINGLE_POINT_INDEX_FAIL);
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void deleteSecondaryIndexEntries(IndexProto.DeleteSecondaryIndexEntriesRequest request,
                                   StreamObserver<IndexProto.DeleteSecondaryIndexEntriesResponse> responseObserver)
    {
        List<IndexProto.IndexKey> keys = request.getIndexKeysList();
        IndexProto.DeleteSecondaryIndexEntriesResponse.Builder builder = IndexProto.DeleteSecondaryIndexEntriesResponse.newBuilder();
        try
        {
            List<Long> rowIds = singlePointIndex.deleteEntries(keys);
            builder.setErrorCode(ErrorCode.SUCCESS).addAllTableRowIds(rowIds);
        }
        catch (SinglePointIndexException e)
        {
            builder.setErrorCode(ErrorCode.INDEX_DELETE_SINGLE_POINT_INDEX_FAIL);
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void updatePrimaryIndexEntry(IndexProto.UpdatePrimaryIndexEntryRequest request,
                                        StreamObserver<IndexProto.UpdatePrimaryIndexEntryResponse> responseObserver)
    {
        super.updatePrimaryIndexEntry(request, responseObserver);
    }

    @Override
    public void updatePrimaryIndexEntries(IndexProto.UpdatePrimaryIndexEntriesRequest request,
                                          StreamObserver<IndexProto.UpdatePrimaryIndexEntriesResponse> responseObserver)
    {
        super.updatePrimaryIndexEntries(request, responseObserver);
    }

    @Override
    public void updateSecondaryIndexEntry(IndexProto.UpdateSecondaryIndexEntryRequest request,
                                          StreamObserver<IndexProto.UpdateSecondaryIndexEntryResponse> responseObserver)
    {
        super.updateSecondaryIndexEntry(request, responseObserver);
    }

    @Override
    public void updateSecondaryIndexEntries(IndexProto.UpdateSecondaryIndexEntriesRequest request,
                                            StreamObserver<IndexProto.UpdateSecondaryIndexEntriesResponse> responseObserver)
    {
        super.updateSecondaryIndexEntries(request, responseObserver);
    }

    @Override
    public void flushIndexEntriesOfFile(IndexProto.FlushIndexEntriesOfFileRequest request,
                                        StreamObserver<IndexProto.FlushIndexEntriesOfFileResponse> responseObserver)
    {
        super.flushIndexEntriesOfFile(request, responseObserver);
    }

    @Override
    public void openIndex(IndexProto.OpenIndexRequest request,
                          StreamObserver<IndexProto.OpenIndexResponse> responseObserver)
    {
        super.openIndex(request, responseObserver);
    }

    @Override
    public void closeIndex(IndexProto.CloseIndexRequest request,
                           StreamObserver<IndexProto.CloseIndexResponse> responseObserver)
    {
        super.closeIndex(request, responseObserver);
    }

    @Override
    public void removeIndex(IndexProto.RemoveIndexRequest request,
                            StreamObserver<IndexProto.RemoveIndexResponse> responseObserver)
    {
        super.removeIndex(request, responseObserver);
    }
}
