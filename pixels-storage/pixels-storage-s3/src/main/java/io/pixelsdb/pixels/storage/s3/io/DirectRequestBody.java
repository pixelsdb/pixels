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
package io.pixelsdb.pixels.storage.s3.io;

import software.amazon.awssdk.core.internal.util.Mimetype;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.ContentStreamProvider;

import java.io.ByteArrayInputStream;

/**
 * The request body of put object that directly passes a byte array to S3 without memory copying.
 * The {@link #fromBytesDirect(byte[], int, int)} method does this, and this method should only be used if the
 * byte array is not modified before it is fully send to S3.
 *
 * @author hank
 * @create 2025-09-22
 */
public class DirectRequestBody extends RequestBody
{
    protected DirectRequestBody(ContentStreamProvider contentStreamProvider, Long contentLength, String contentType)
    {
        super(contentStreamProvider, contentLength, contentType);
    }

    /**
     * Construct a request body without memory copying.
     * @return the request body
     */
    public static RequestBody fromBytesDirect(byte[] bytes, int offset, int length)
    {
        return new DirectRequestBody(() ->
                new ByteArrayInputStream(bytes, offset, length), (long) length, Mimetype.MIMETYPE_OCTET_STREAM);
    }
}
