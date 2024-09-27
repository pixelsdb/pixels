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
import io.pixelsdb.pixels.common.exception.EtcdException;
import io.pixelsdb.pixels.common.utils.EtcdUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static io.pixelsdb.pixels.common.utils.Constants.AI_LOCK_PATH_PREFIX;

/**
 * @author hank
 * @create 2021-08-29
 */
public class EtcdAutoIncrement
{
    private static final Logger logger = LogManager.getLogger(EtcdAutoIncrement.class);

    private EtcdAutoIncrement() { }

    /**
     * Initialize the id (set init value to '1') by the id key.
     * This method is idempotent.
     * @param idKey the key of the auto-increment id
     */
    public static void InitId(String idKey) throws EtcdException
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
                // Issue #729: set init value to 1 instead of 0.
                etcd.putKeyValue(idKey, "1");
            }
        }
        catch (EtcdException e)
        {
            logger.error(e);
            throw new EtcdException("failed to initialize the id key", e);
        }
        finally
        {
            try
            {
                writeLock.release();
            }
            catch (EtcdException e)
            {
                logger.error(e);
                throw new EtcdException("failed to release write lock", e);
            }
        }
    }

    /**
     * Get a new incremental id and increase the id in etcd by 1.
     * @param idKey the key of the auto-increment id
     * @return the new id
     * @throws EtcdException if failed to interact with etcd
     */
    public static long GenerateId(String idKey) throws EtcdException
    {
        Segment segment = GenerateId(idKey, 1);
        if (segment.isValid() && segment.length == 1)
        {
            return segment.getStart();
        }
        return 0;
    }

    /**
     * Get a new segment of incremental ids and increase the id in etcd by the step.
     * @param idKey the key of the auto-increment id
     * @param step the step, i.e., the number of ids to get
     * @return the segment of auto-increment ids
     * @throws EtcdException if failed to interact with etcd
     */
    public static Segment GenerateId(String idKey, long step) throws EtcdException
    {
        Segment segment;
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
                long start = Long.parseLong(new String(idKV.getValue().getBytes()));
                start += step;
                etcd.putKeyValue(idKey, String.valueOf(start));
                segment = new Segment(start, step);
                if (!segment.isValid())
                {
                    throw new EtcdException("invalid segment for id key " + idKey + " from etcd");
                }
            }
            else
            {
                throw new EtcdException("the key value of the id " + idKey + " does not exist in etcd");
            }
        }
        catch (EtcdException e)
        {
            logger.error(e);
            throw new EtcdException("failed to increment the id", e);
        }
        finally
        {
            try
            {
                writeLock.release();
            } catch (EtcdException e)
            {
                logger.error(e);
                throw new EtcdException("failed to release write lock", e);
            }
        }
        return segment;
    }

    /**
     * Get a new segment of incremental ids and increase the id in etcd by the step.
     * @param idKey the key of the auto-increment id
     * @param step the step, i.e., the number of ids to get
     * @param processor the processor of the segment acquired from etcd
     * @throws EtcdException if failed to interact with etcd
     */
    public static void GenerateId(String idKey, long step, SegmentProcessor processor) throws EtcdException
    {
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
                long start = Long.parseLong(new String(idKV.getValue().getBytes()));
                start += step;
                etcd.putKeyValue(idKey, String.valueOf(start));
                Segment segment = new Segment(start, step);
                if (!segment.isValid())
                {
                    throw new EtcdException("invalid segment for id key " + idKey + " from etcd");
                }
                processor.process(segment);
            }
            else
            {
                throw new EtcdException("the key value of the id " + idKey + " does not exist in etcd");
            }
        }
        catch (EtcdException e)
        {
            logger.error(e);
            throw new EtcdException("failed to increment the id", e);
        }
        finally
        {
            try
            {
                writeLock.release();
            } catch (EtcdException e)
            {
                logger.error(e);
                throw new EtcdException("failed to release write lock", e);
            }
        }
    }

    public static class Segment
    {
        private final long start;
        private final long length;

        public Segment(long start, long length)
        {
            this.start = start;
            this.length = length;
        }

        public long getStart()
        {
            return start;
        }

        public long getLength()
        {
            return length;
        }

        public boolean isValid()
        {
            return this.start > 0 && this.length > 0;
        }

        public boolean isEmpty()
        {
            return this.length > 0;
        }
    }

    /**
     * The processor to be called to process the segment acquired from etcd.
     */
    public static interface SegmentProcessor
    {
        /**
         * @param segment the segment acquired from etcd, must be valid
         */
        void process(Segment segment);
    }
}
