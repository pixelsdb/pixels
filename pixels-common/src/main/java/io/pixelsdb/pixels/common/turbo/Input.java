/*
 * Copyright 2022 PixelsDB.
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

import static java.util.Objects.requireNonNull;

/**
 * The base class for the input of a cloud function (serverless) worker.
 * @author hank
 * @create 2022-06-28
 */
public abstract class Input
{
    /**
     * The unique transaction id of the query.
     */
    private long transId;

    /**
     * The transaction timestamp of the query.
     */
    private long timestamp;
    
    /**
     * The stage id of this function worker.
     */
    private int stageId;

    /**
     * The operator name of this function worker. This is optional and might be null.
     */
    private String operatorName;

    /**
     * The required CPU of each function worker. Every 1024 represents a vCPU.
     */
    private int requiredCpu;

    /**
     * The required memory capacity in MB of each function worker.
     */
    private int requiredMemory;

    public Input(long transId, long timestamp)
    {
        this.transId = transId;
        this.timestamp = timestamp;
        // Issue #468: operatorName is optional, it is to be set by the setter.
        this.operatorName = null;
        this.stageId = -1;
        this.requiredCpu = 0;
        this.requiredMemory = 0;
    }

    public long getTransId()
    {
        return transId;
    }

    public void setTransId(long transId)
    {
        this.transId = transId;
    }

    public long getTimestamp()
    {
        return timestamp;
    }

    public void setTimestamp(long timestamp)
    {
        this.timestamp = timestamp;
    }
  
    public int getStageId()
    {
        return stageId;
    }
  
    public void setStageId(int stageId)
    {
        this.stageId = stageId;
    }

    public String getOperatorName()
    {
        return operatorName;
    }

    public void setOperatorName(String operatorName)
    {
        this.operatorName = requireNonNull(operatorName, "operatorName is null");
    }

    public int getRequiredCpu()
    {
        return requiredCpu;
    }

    public void setRequiredCpu(int requiredCpu)
    {
        this.requiredCpu = requiredCpu;
    }

    public int getRequiredMemory()
    {
        return requiredMemory;
    }

    public void setRequiredMemory(int requiredMemory)
    {
        this.requiredMemory = requiredMemory;
    }
}
