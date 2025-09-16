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
package io.pixelsdb.pixels.storage.http.io;

import io.netty.buffer.ByteBuf;

/**
 * @author hank
 * @create 2025-09-16
 */
public class HttpContent implements Comparable<HttpContent>
{
    private final int partId;
    private final ByteBuf content;

    public HttpContent(int partId, ByteBuf content)
    {
        this.partId = partId;
        this.content = content;
    }

    public int getPartId()
    {
        return partId;
    }

    public ByteBuf getContent()
    {
        return content;
    }

    @Override
    public int compareTo(HttpContent that)
    {
        return Integer.compare(this.partId, that.partId);
    }
}
