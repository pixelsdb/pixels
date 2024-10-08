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
package io.pixelsdb.pixels.planner.plan.physical.domain;

import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.utils.ConfigFactory;

/**
 * @author hank
 * @create 2023-06-05
 */
public class StorageInfoBuilder
{
    private StorageInfoBuilder() { }

    public static StorageInfo BuildFromConfig(Storage.Scheme scheme)
    {
        String region, endpoint, accessKey, secretKey;
        switch (scheme)
        {
            case minio:
                region = ConfigFactory.Instance().getProperty("minio.region");
                endpoint = ConfigFactory.Instance().getProperty("minio.endpoint");
                accessKey = ConfigFactory.Instance().getProperty("minio.access.key");
                secretKey = ConfigFactory.Instance().getProperty("minio.secret.key");
                break;
            case redis:
                region = "";
                endpoint = ConfigFactory.Instance().getProperty("redis.endpoint");
                accessKey = ConfigFactory.Instance().getProperty("redis.access.key");
                secretKey = ConfigFactory.Instance().getProperty("redis.secret.key");
                break;
            case mock:  // HTTP storage info. FIXME: This format is only for dev
                region = "http"; endpoint = ""; accessKey = ""; secretKey = "";
                break;
            default:
                region = ""; endpoint = ""; accessKey = ""; secretKey = "";
                break;
        }
        return new StorageInfo(scheme, region, endpoint, accessKey, secretKey);
    }
}
