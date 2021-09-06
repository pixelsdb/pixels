/*
 * Copyright 2021 PixelsDB.
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

import io.etcd.jetcd.KeyValue;
import io.pixelsdb.pixels.common.utils.EtcdUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static io.pixelsdb.pixels.common.utils.Constants.AI_LOCK_PATH_PREFIX;

/**
 * Created at: 8/29/21
 * Author: hank
 */
public class EtcdAutoIncrement
{
    private static final Logger logger = LogManager.getLogger(EtcdAutoIncrement.class);

    private EtcdAutoIncrement() { }

    /**
     * Initialize the id (set init value to '0') by the id key.
     * This method is idempotent.
     * @param idKey
     */
    public static void InitId(String idKey)
    {
        EtcdUtil etcd = EtcdUtil.Instance();
        EtcdReadWriteLock readWriteLock = new EtcdReadWriteLock(etcd.getClient(),
                AI_LOCK_PATH_PREFIX + idKey);
        EtcdMutex writeLock = readWriteLock.writeLock();
        try
        {
            writeLock.acquire();
            KeyValue idKV = etcd.getKeyValue(idKey);
            if (idKV == null)
            {
                etcd.putKeyValue(idKey, "0");
            }
        } catch (Exception e)
        {
            logger.error(e);
            e.printStackTrace();
        } finally
        {
            try
            {
                writeLock.release();
            } catch (Exception e)
            {
                logger.error(e);
                e.printStackTrace();
            }
        }
    }

    public static long GenerateId(String idKey)
    {
        long id = 0;
        EtcdUtil etcd = EtcdUtil.Instance();
        EtcdReadWriteLock readWriteLock = new EtcdReadWriteLock(etcd.getClient(),
                AI_LOCK_PATH_PREFIX + idKey);
        EtcdMutex writeLock = readWriteLock.writeLock();
        try
        {
            writeLock.acquire();
            KeyValue idKV = etcd.getKeyValue(idKey);
            if (idKV != null)
            {
                id = Long.parseLong(new String(idKV.getValue().getBytes()));
                id++;
                etcd.putKeyValue(idKey, id + "");
            }
        } catch (Exception e)
        {
            logger.error(e);
            e.printStackTrace();
        } finally
        {
            try
            {
                writeLock.release();
            } catch (Exception e)
            {
                logger.error(e);
                e.printStackTrace();
            }
        }
        return id;
    }
}
