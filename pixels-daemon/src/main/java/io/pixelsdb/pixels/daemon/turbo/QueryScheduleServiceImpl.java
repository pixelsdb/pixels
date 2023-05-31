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
package io.pixelsdb.pixels.daemon.turbo;

import io.grpc.stub.StreamObserver;
import io.pixelsdb.pixels.turbo.QueryScheduleServiceGrpc;
import io.pixelsdb.pixels.turbo.TurboProto;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author hank
 * @create 2023-05-31
 */
public class QueryScheduleServiceImpl extends QueryScheduleServiceGrpc.QueryScheduleServiceImplBase
{
    private static final Logger log = LogManager.getLogger(QueryScheduleServiceImpl.class);

    @Override
    public void scheduleQuery(TurboProto.ScheduleQueryRequest request,
                              StreamObserver<TurboProto.ScheduleQueryResponse> responseObserver)
    {
        super.scheduleQuery(request, responseObserver);
    }

    @Override
    public void finishQuery(TurboProto.FinishQueryRequest request,
                            StreamObserver<TurboProto.FinishQueryResponse> responseObserver)
    {
        super.finishQuery(request, responseObserver);
    }

    @Override
    public void getQuerySlots(TurboProto.GetQuerySlotsRequest request,
                              StreamObserver<TurboProto.GetQuerySlotsResponse> responseObserver)
    {
        super.getQuerySlots(request, responseObserver);
    }
}
