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
package io.pixelsdb.pixels.common.turbo;

import com.alibaba.fastjson.annotation.JSONField;
import com.alibaba.fastjson.annotation.JSONType;

/**
 * The request sent to Spike workers. Such a request is serialized to a Json string and sent as the payload to
 * the Spike serverless computing framework.
 */
@JSONType
public class SpikeWorkerRequest
{
    @JSONField(name = "workerType")
    private WorkerType workerType;

    @JSONField(name = "workerPayload")
    private String workerPayload;

    public SpikeWorkerRequest() { }

    public SpikeWorkerRequest(WorkerType workerType, String workerPayload)
    {
        this.workerType = workerType;
        this.workerPayload = workerPayload;
    }

    public String getWorkerPayload()
    {
        return workerPayload;
    }

    public void setWorkerPayload(String workerPayload)
    {
        this.workerPayload = workerPayload;
    }

    public WorkerType getWorkerType()
    {
        return workerType;
    }

    public void setWorkerType(WorkerType workerType)
    {
        this.workerType = workerType;
    }
}
