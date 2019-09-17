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
package io.pixelsdb.pixels.common.lock;

import io.pixelsdb.pixels.common.utils.EtcdUtil;
import com.coreos.jetcd.Client;
import org.junit.Test;

/**
 * Created at: 18-10-28
 * Author: hank
 */
public class TestEtcdLock
{
    @Test
    public void delete()
    {
        EtcdUtil etcdUtil = EtcdUtil.Instance();
        String basePath = "/read-write-lock";
        etcdUtil.deleteByPrefix(basePath);
    }

    @Test
    public void testReadLock() throws Exception
    {
        EtcdUtil etcdUtil = EtcdUtil.Instance();
        Client client = etcdUtil.getClient();
        String basePath = "/read-write-lock";

        EtcdReadWriteLock readWriteLock = new EtcdReadWriteLock(client, basePath);
        EtcdMutex readLock = readWriteLock.readLock();
        readLock.acquire();
        System.err.println("get read lock");
        Thread.sleep(2000);
        System.err.println("release read lock");
        readLock.release();
    }

    @Test
    public void testWriteLock() throws Exception
    {
        EtcdUtil etcdUtil = EtcdUtil.Instance();
        Client client = etcdUtil.getClient();
        String basePath = "/read-write-lock";

        EtcdReadWriteLock readWriteLock = new EtcdReadWriteLock(client, basePath);
        EtcdMutex writeLock = readWriteLock.writeLock();
        writeLock.acquire();
        System.err.println("get write lock");
        Thread.sleep(20000);
        System.err.println("release write lock");
        writeLock.release();
    }
}
