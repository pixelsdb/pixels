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
package io.pixelsdb.pixels.presto;

import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author hank
 * Finished at: 20/02/2022
 */
public class PixelsTransactionHandle
        implements ConnectorTransactionHandle
{
    public static final PixelsTransactionHandle Default = new PixelsTransactionHandle(-1, -1);

    private long transId;
    private long timestamp;

    /**
     * Create a transaction handle.
     * @param transId is also the queryId as a query is a single-statement read-only transaction.
     * @param timestamp the timestamp of a transaction.
     */
    @JsonCreator
    public PixelsTransactionHandle(@JsonProperty("transId") long transId,
                                   @JsonProperty("timestamp") long timestamp)
    {
        this.transId = transId;
        this.timestamp = timestamp;
    }

    @JsonProperty
    public long getTransId()
    {
        return this.transId;
    }

    @JsonProperty
    public long getTimestamp()
    {
        return this.timestamp;
    }
}
