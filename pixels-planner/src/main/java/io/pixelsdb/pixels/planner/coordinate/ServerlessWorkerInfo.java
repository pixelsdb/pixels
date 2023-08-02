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
package io.pixelsdb.pixels.planner.coordinate;

import io.pixelsdb.pixels.common.task.WorkerInfo;
import io.pixelsdb.pixels.turbo.TurboProto;

/**
 * @author hank
 * @create 2023-08-02
 */
public class ServerlessWorkerInfo implements WorkerInfo
{
    private final String ip;
    private final int streamPort;
    private final long transId;
    private final String operatorName;
    private final int hashBucketId;

    public ServerlessWorkerInfo(String ip, int streamPort, long transId, String operatorName, int hashBucketId)
    {
        this.ip = ip;
        this.streamPort = streamPort;
        this.transId = transId;
        this.operatorName = operatorName;
        this.hashBucketId = hashBucketId;
    }

    public String getIp()
    {
        return ip;
    }

    public int getStreamPort()
    {
        return streamPort;
    }

    public long getTransId()
    {
        return transId;
    }

    public String getOperatorName()
    {
        return operatorName;
    }

    public int getHashBucketId()
    {
        return hashBucketId;
    }

    public TurboProto.WorkerInfo toProto()
    {
        TurboProto.WorkerInfo.Builder builder = TurboProto.WorkerInfo.newBuilder()
                .setIp(this.ip).setStreamPort(this.streamPort)
                .setTransId(this.transId).setOperatorName(this.operatorName);
        if (this.hashBucketId >= 0)
        {
            builder.setHashBucketId(this.hashBucketId);
        }
        return builder.build();
    }
}
