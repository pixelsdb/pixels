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
package io.pixelsdb.pixels.executor.plan;

import io.pixelsdb.pixels.common.physical.Storage;

/**
 * @author hank
 * @date 05/07/2022
 */
public class OutputEndPoint
{
    private final Storage.Scheme scheme;
    private final String folder;
    private final String accessKey;
    private final String secretKey;
    private final String endPoint;

    public OutputEndPoint(Storage.Scheme scheme, String folder,
                          String accessKey, String secretKey, String endPoint)
    {
        this.scheme = scheme;
        this.folder = folder;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.endPoint = endPoint;
    }

    public Storage.Scheme getScheme()
    {
        return scheme;
    }

    public String getFolder()
    {
        return folder;
    }

    public String getAccessKey()
    {
        return accessKey;
    }

    public String getSecretKey()
    {
        return secretKey;
    }

    public String getEndPoint()
    {
        return endPoint;
    }
}
