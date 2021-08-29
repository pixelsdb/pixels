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
package io.pixelsdb.pixels.daemon.metadata;

import io.pixelsdb.pixels.common.lock.EtcdMutex;
import io.pixelsdb.pixels.common.lock.EtcdReadWriteLock;
import io.pixelsdb.pixels.common.utils.EtcdUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import static io.pixelsdb.pixels.daemon.metadata.dao.impl.EtcdCommon.tableIdLockPath;

/**
 * @author: tao
 * @date: Create in 2018-01-27 10:46
 **/
public class TestServer {

    @Test
    public void test() {
        MetadataServer server = new MetadataServer(18888);
        server.run();
    }



    class WriteLock implements Runnable
    {

        @Override
        public void run()
        {
            EtcdUtil etcd = EtcdUtil.Instance();
            Logger log = LogManager.getLogger(WriteLock.class);
            EtcdReadWriteLock readWriteLock = new EtcdReadWriteLock(etcd.getClient(),
                    tableIdLockPath);
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

    class ReadLock implements Runnable
    {

        @Override
        public void run()
        {
            EtcdUtil etcd = EtcdUtil.Instance();
            Logger log = LogManager.getLogger(ReadLock.class);
            EtcdReadWriteLock readWriteLock = new EtcdReadWriteLock(etcd.getClient(),
                    tableIdLockPath);
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
        Thread t1 = new Thread(new WriteLock());
        Thread t2 = new Thread(new WriteLock());
        t1.start();
        Thread.sleep(2000);
        t2.start();
        t1.join();
        t2.join();
    }
}
