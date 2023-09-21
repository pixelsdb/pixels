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

import java.util.List;

/**
 * @author hank
 * @create 2023-08-02
 */
public class ServerlessWorkerInfo implements WorkerInfo
{
    private final String ip;
    private final int port;
    private final long transId;
    private final String operatorName;
    private final List<Integer> hashValues;

    public ServerlessWorkerInfo(String ip, int port, long transId, String operatorName, List<Integer> hashValues)
    {
        this.ip = ip;
        this.port = port;
        this.transId = transId;
        this.operatorName = operatorName;
        this.hashValues = hashValues;
    }

    public String getIp()
    {
        return ip;
    }

    public int getPort()
    {
        return port;
    }

    public long getTransId()
    {
        return transId;
    }

    public String getOperatorName()
    {
        return operatorName;
    }

    public List<Integer> getHashValues()
    {
        return hashValues;
    }

    public TurboProto.WorkerInfo toProto()
    {
        TurboProto.WorkerInfo.Builder builder = TurboProto.WorkerInfo.newBuilder()
                .setIp(this.ip).setPort(this.port)
                .setTransId(this.transId).setOperatorName(this.operatorName);
        if (this.hashValues != null && !this.hashValues.isEmpty())
        {
            builder.addAllHashValues(this.hashValues);
        }
        return builder.build();
    }
}
