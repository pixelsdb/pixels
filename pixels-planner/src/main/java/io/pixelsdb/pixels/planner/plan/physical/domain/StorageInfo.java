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
package io.pixelsdb.pixels.planner.plan.physical.domain;

import io.pixelsdb.pixels.common.physical.Storage;

/**
 * The information of the storage endpoint, such as S3 or Minio.
 * @author hank
 * @create 2022-07-06
 */
public class StorageInfo
{
    /**
     * The storage scheme of the storage, e.g., s3, minio.
     */
    private Storage.Scheme scheme;
    /**
     * The region of the storage, e.g., eu-central-2.
     */
    private String region;
    /**
     * The endpoint of the storage, e.g., http://hostname:port.
     */
    private String endpoint;
    /**
     * The access key of the storage.
     */
    private String accessKey;
    /**
     * The secret key of the storage.
     */
    private String secretKey;

    /**
     * Default constructor for Jackson.
     */
    public StorageInfo() { }

    public StorageInfo(Storage.Scheme scheme, String region,
                       String endpoint, String accessKey, String secretKey)
    {
        this.scheme = scheme;
        this.region = region;
        this.endpoint = endpoint;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
    }

    public Storage.Scheme getScheme()
    {
        return scheme;
    }

    public void setScheme(Storage.Scheme scheme)
    {
        this.scheme = scheme;
    }

    public String getRegion()
    {
        return region;
    }

    public void setRegion(String region)
    {
        this.region = region;
    }

    public String getEndpoint()
    {
        return endpoint;
    }

    public void setEndpoint(String endpoint)
    {
        this.endpoint = endpoint;
    }

    public String getAccessKey()
    {
        return accessKey;
    }

    public void setAccessKey(String accessKey)
    {
        this.accessKey = accessKey;
    }

    public String getSecretKey()
    {
        return secretKey;
    }

    public void setSecretKey(String secretKey)
    {
        this.secretKey = secretKey;
    }
}
