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
import io.pixelsdb.pixels.index.IndexProto;
import io.pixelsdb.pixels.index.IndexServiceGrpc;

/**
 * @author hank
 * @create 2025-02-19
 */
public class IndexServiceImpl extends IndexServiceGrpc.IndexServiceImplBase
{
    // TODO: implement

    @Override
    public void lookupUniqueIndex(IndexProto.LookupUniqueIndexRequest request,
                            StreamObserver<IndexProto.LookupUniqueIndexResponse> responseObserver)
    {
        super.lookupUniqueIndex(request, responseObserver);
    }

    @Override
    public void lookupNonUniqueIndex(IndexProto.LookupNonUniqueIndexRequest request,
                                  StreamObserver<IndexProto.LookupNonUniqueIndexResponse> responseObserver)
    {
        super.lookupNonUniqueIndex(request, responseObserver);
    }

    @Override
    public void putIndexEntry(IndexProto.PutIndexEntryRequest request,
                              StreamObserver<IndexProto.PutIndexEntryResponse> responseObserver)
    {
        super.putIndexEntry(request, responseObserver);
    }

    @Override
    public void deleteIndexEntry(IndexProto.DeleteIndexEntryRequest request,
                                 StreamObserver<IndexProto.DeleteIndexEntryResponse> responseObserver)
    {
        super.deleteIndexEntry(request, responseObserver);
    }

    @Override
    public void putIndexEntries(IndexProto.PutIndexEntriesRequest request,
                                StreamObserver<IndexProto.PutIndexEntriesResponse> responseObserver)
    {
        super.putIndexEntries(request, responseObserver);
    }

    @Override
    public void deleteIndexEntries(IndexProto.DeleteIndexEntriesRequest request,
                                   StreamObserver<IndexProto.DeleteIndexEntriesResponse> responseObserver)
    {
        super.deleteIndexEntries(request, responseObserver);
    }
}
