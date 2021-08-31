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
import io.etcd.jetcd.Client;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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

    class RunWriteLock implements Runnable
    {

        @Override
        public void run()
        {
            EtcdUtil etcd = EtcdUtil.Instance();
            Logger log = LogManager.getLogger(RunWriteLock.class);
            EtcdReadWriteLock readWriteLock = new EtcdReadWriteLock(etcd.getClient(),
                    "/read-write-lock");
            EtcdMutex writeLock = readWriteLock.writeLock().verbose();
            try
            {
                writeLock.acquire();
                System.out.println(this +", lock success.");
            } catch (Exception e)
            {
                log.error(e);
                e.printStackTrace();
            }
            finally
            {
                try
                {
                    Thread.sleep(10000);
                    writeLock.release();
                } catch (Exception e)
                {
                    log.error(e);
                    e.printStackTrace();
                }
            }
        }
    }

    class RunReadLock implements Runnable
    {

        @Override
        public void run()
        {
            EtcdUtil etcd = EtcdUtil.Instance();
            Logger log = LogManager.getLogger(RunReadLock.class);
            EtcdReadWriteLock readWriteLock = new EtcdReadWriteLock(etcd.getClient(),
                    "/read-write-lock");
            EtcdMutex readLock = readWriteLock.readLock().verbose();
            try
            {
                readLock.acquire();
                System.out.println(this + ", lock success.");
            } catch (Exception e)
            {
                log.error(e);
                e.printStackTrace();
            }
            finally
            {
                try
                {
                    readLock.release();
                } catch (Exception e)
                {
                    log.error(e);
                    e.printStackTrace();
                }
            }
        }
    }

    @Test
    public void testEtcdLock() throws InterruptedException
    {
        Thread t1 = new Thread(new RunWriteLock());
        Thread t2 = new Thread(new RunWriteLock());
        t1.start();
        Thread.sleep(2000);
        t2.start();
        t1.join();
        t2.join();
    }
}
