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

import io.etcd.jetcd.Client;

/**
 * @author tao
 * @author hank
 * @create 2018-10-27 14:29
 **/
public class EtcdReadWriteLock
{
    private EtcdMutex readMutex;
    private EtcdMutex writeMutex;

    private final Client client;
    private final String basePath;
    private final byte[] lockData;

    private static final String READ_LOCK_NAME = "_READ_";
    private static final String WRITE_LOCK_NAME = "_WRIT_";

    /**
     * @param client   the client
     * @param basePath path to use for locking
     */
    public EtcdReadWriteLock(Client client, String basePath)
    {
        this(client, basePath, null);
    }

    /**
     * @param client   the client
     * @param basePath path to use for locking
     * @param lockData the data to store in the lock nodes
     */
    public EtcdReadWriteLock(Client client, String basePath, byte[] lockData)
    {
        this.client = client;
        this.basePath = basePath;
        this.lockData = lockData;
        this.readMutex = null;
        this.writeMutex = null;
    }

    /**
     * Returns the lock used for reading.
     *
     * @return read lock
     */
    public EtcdMutex readLock()
    {
        if (this.readMutex == null)
        {
            this.readMutex = new EtcdReadWriteLock.InternalInterProcessMutex(client, basePath, READ_LOCK_NAME, lockData);
        }
        return this.readMutex;
    }

    /**
     * Returns the lock used for writing.
     *
     * @return write lock
     */
    public EtcdMutex writeLock()
    {
        if (this.writeMutex == null)
        {
            this.writeMutex = new EtcdReadWriteLock.InternalInterProcessMutex(client, basePath, WRITE_LOCK_NAME, lockData);
        }
        return this.writeMutex;
    }

    private static class InternalInterProcessMutex extends EtcdMutex
    {
        private final String lockName;
        private final byte[] lockData;

        InternalInterProcessMutex(Client client, String path, String lockName, byte[] lockData)
        {
            super(client, path, lockName);
            this.lockName = lockName;
            this.lockData = lockData;
        }

        protected byte[] getLockNodeBytes()
        {
            return this.lockData;
        }
    }
}
