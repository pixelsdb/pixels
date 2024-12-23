/*
 * Copyright 2018 PixelsDB.
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

package io.pixelsdb.pixels.daemon.retina;

import io.grpc.stub.StreamObserver;
import io.pixelsdb.pixels.retina.RetinaWorkerServiceGrpc;
import io.pixelsdb.pixels.retina.RetinaProto;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Created at: 24-12-20
 * Author: gengdy
 */
public class RetinaServerImpl extends RetinaWorkerServiceGrpc.RetinaWorkerServiceImplBase
{
    private static final Logger logger = LogManager.getLogger(RetinaServerImpl.class);

    public RetinaServerImpl() {}

    @Override
    public void updateRecord(RetinaProto.UpdateRecordRequest request,
                             StreamObserver<RetinaProto.UpdateRecordResponse> responseObserver)
    {
        // TODO: Implement updateRecord
    }

    @Override
    public void insertRecord(RetinaProto.InsertRecordRequest request,
                             StreamObserver<RetinaProto.InsertRecordResponse> responseObserver)
    {
        // TODO: Implement insertRecord
    }

    @Override
    public void deleteRecord(RetinaProto.DeleteRecordRequest request,
                             StreamObserver<RetinaProto.DeleteRecordResponse> responseObserver)
    {
        // TODO: Implement DeleteRecord
    }

    @Override
    public void queryRecords(RetinaProto.QueryRecordsRequest request,
                            StreamObserver<RetinaProto.QueryRecordsResponse> responseObserver)
    {
        // TODO: Implement QueryRecords
    }

    @Override
    public void finishRecords(RetinaProto.QueryAck request,
                            StreamObserver<RetinaProto.ResponseHeader> responseObserver)
    {
        // TODO: Implement FinishRecords
    }

    @Override
    public void queryVisibility(RetinaProto.QueryVisibilityRequest request,
                                StreamObserver<RetinaProto.QueryVisibilityResponse> responseObserver)
    {
        // TODO: Implement QueryVisibility
    }

    @Override
    public void finishVisibility(RetinaProto.QueryAck request,
                                StreamObserver<RetinaProto.ResponseHeader> responseObserver)
    {
        // TODO: Implement FinishVisibility
    }
}
