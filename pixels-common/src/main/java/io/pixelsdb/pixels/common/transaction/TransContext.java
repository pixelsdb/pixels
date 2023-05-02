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
package io.pixelsdb.pixels.common.transaction;

import io.pixelsdb.pixels.daemon.TransProto;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * @create 2022-02-20
 * @author hank
 */
public class TransContext
{
    private final long transId;
    private final long timestamp;
    private final boolean readOnly;
    private final AtomicReference<TransProto.TransStatus> status;
    private final Properties properties;

    public TransContext(long transId, long timestamp, boolean readOnly)
    {
        this.transId = transId;
        this.timestamp = timestamp;
        this.readOnly = readOnly;
        this.status = new AtomicReference<>(TransProto.TransStatus.PENDING);
        this.properties = new Properties();
    }

    public long getTransId()
    {
        return transId;
    }

    public long getTimestamp()
    {
        return timestamp;
    }

    public TransProto.TransStatus getStatus()
    {
        return status.get();
    }

    public boolean isReadOnly()
    {
        return readOnly;
    }

    public void setStatus(TransProto.TransStatus status)
    {
        this.status.set(status);
    }

    public Properties getProperties()
    {
        return this.properties;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("transId", transId)
                .add("timestamp", timestamp)
                .add("status", status.get())
                .toString();
    }
}
