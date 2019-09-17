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

import io.pixelsdb.pixels.common.utils.EtcdUtil;
import com.coreos.jetcd.data.KeyValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * Created at: 18-11-25
 * Author: hank
 */
public class TestCache
{
    @Before
    public void loadEtcd ()
    {
        EtcdUtil etcdUtil = EtcdUtil.Instance();
        etcdUtil.putKeyValue("cache_version", "1");
        etcdUtil.putKeyValue("location_1_dbiir11", "file1;file2;file3;file4;file5");
    }

    @Test
    public void getNodeFiles ()
    {
        String cacheVersion;
        EtcdUtil etcdUtil = EtcdUtil.Instance();
        KeyValue keyValue = etcdUtil.getKeyValue("cache_version");
        if(keyValue != null)
        {
            // 1. get version
            cacheVersion = keyValue.getValue().toStringUtf8();
            System.out.println("cache_version: " + cacheVersion);
            // 2. get files of each node
            List<KeyValue> nodeFiles = etcdUtil.getKeyValuesByPrefix("location_" + cacheVersion);
            if(nodeFiles.size() > 0)
            {
                for (KeyValue kv : nodeFiles)
                {
                    String node = kv.getKey().toStringUtf8().split("_")[2];
                    String[] files = kv.getValue().toStringUtf8().split(";");
                    for(String file : files)
                    {
                        System.out.println(file + ", " + node);
                    }
                }
            }
            else
            {
                System.out.println("Get caching files error when version is " + cacheVersion);
                System.exit(-1);
            }
        }
        else
        {
            System.out.println("Get caching version error. ");
            System.exit(-1);
        }
    }

    @After
    public void deleteEtcd ()
    {
        EtcdUtil etcdUtil = EtcdUtil.Instance();
        etcdUtil.delete("cache_version");
        etcdUtil.delete("location_1_dbiir11");
    }
}
