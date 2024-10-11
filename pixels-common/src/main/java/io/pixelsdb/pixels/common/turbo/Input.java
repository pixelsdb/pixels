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
 * The base class for the input of a cloud function.
 * @author hank
 * @create 2022-06-28
 */
public abstract class Input
{
    /**
     * The unique id of the transaction.
     */
    private long transId;

    /**
     * The timestamp of the transaction.
     */
    private long timestamp;

    private String operatorName;

    public Input(long transId, long timestamp)
    {
        this.transId = transId;
        this.timestamp = timestamp;
        // Issue #468: operatorName is optional, it is to be set by the setter.
        this.operatorName = null;
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

    /**
     * Operator name is optional, it might be null if not set.
     * @return the operator name.
     */
    public String getOperatorName()
    {
        return operatorName;
    }

    public void setOperatorName(String operatorName)
    {
        this.operatorName = requireNonNull(operatorName, "operatorName is null");
    }
}
