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
package io.pixelsdb.pixels.cache.mq;

import io.pixelsdb.pixels.cache.MemoryMappedFile;
import static io.pixelsdb.pixels.common.error.ErrorCode.*;

/**
 * The message queue that can be used to collect cache miss messages,
 * or for low-latency interprocess communications on the same node.
 * It should be backed by an in-memory file for high performance.
 * Created at: 3/19/21 (at DIAS EPFL)
 * Author: bian
 */
public class SharedMQ
{
    public static class TransactionStatus
    {
        public static final byte CLEAR = 0;
        public static final byte RUNNING = 1;
        public static final byte ROLLBACK = 2;
        public static final byte COMMIT = 3;
    }

    public static final long SHARED_MQ_STRUCT_READ_LIMIT = 0L;
    public static final long SHARED_MQ_STRUCT_WRITE_LIMIT = 8L;
    public static final long SHARED_MQ_STRUCT_INIT_FLAG = 16L;
    // although only use 17 bytes are used for metadata, we use 24 bytes for word alignment.
    public static final long SHARED_MQ_STRUCT_DATA = 24L;
    public static final long SHARED_MQ_LIMIT_FLAG = 0x4000000000000000L;
    public static final long SHARED_MQ_LIMIT_MASK = 0x3FFFFFFFFFFFFFFFL;
    public static final int SHARED_MQ_LENGTH_MESSAGE_HEADER = 2;
    public static final int SHARED_MQ_LENGTH_MESSAGE_STATUS = 1;
    public static final int SHARED_MQ_DEFAULT_TIMEOUT_MS = 1000;
    public static final byte SHARED_MQ_INITIALIZED = 0x66;

    public static int SHARED_MQ_ENTRY_SIZE(int messageSize)
    {
        return SHARED_MQ_LENGTH_MESSAGE_HEADER + messageSize;
    }


    public static long SHARED_MQ_REAL_LIMIT(long limit)
    {
        return SHARED_MQ_LIMIT_MASK & limit;
    }

    public static long SHARED_MQ_FLIP_LIMIT(long limit)
    {
        return (((limit ^ SHARED_MQ_LIMIT_FLAG) & SHARED_MQ_LIMIT_FLAG) | SHARED_MQ_STRUCT_DATA);
    }

    public static boolean SHARED_MQ_IS_FLIPPED(long read_limit, long write_limit)
    {
        return ((read_limit & SHARED_MQ_LIMIT_FLAG) ^ (write_limit & SHARED_MQ_LIMIT_FLAG)) != 0;
    }

    private MemoryMappedFile _sharedMemory;
    private int _messageSize;
    private int _timeout;
    private boolean _readLimitPushed;
    private boolean _writeLimitPushed;
    private long _readLimit;
    private long _writeLimit;
    private long _readStartTime;
    private long _writeStartTime;
    private long _EOQ; // end of queue.

    public SharedMQ (String location, long totalSize, int messageSize) throws Exception
    {
        this(location, totalSize, messageSize, SHARED_MQ_DEFAULT_TIMEOUT_MS, false);
    }

    public SharedMQ (String location, long totalSize, int messageSize, boolean forceInit) throws Exception
    {
        this(location, totalSize, messageSize, SHARED_MQ_DEFAULT_TIMEOUT_MS, forceInit);
    }

    public SharedMQ (String location, long totalSize, int messageSize, int timeout, boolean forceInit) throws Exception
    {
        _timeout = timeout;
        _messageSize = messageSize;
        _readLimitPushed = false;
        _writeLimitPushed = false;
        _readLimit = SHARED_MQ_STRUCT_DATA;
        _writeLimit = SHARED_MQ_STRUCT_DATA;
        _readStartTime = 0;
        _writeStartTime = 0;
        _sharedMemory = new MemoryMappedFile(location, totalSize);
        _EOQ = _sharedMemory.getSize();
        _EOQ -= (_EOQ - SHARED_MQ_STRUCT_DATA) % SHARED_MQ_ENTRY_SIZE(_messageSize);
        init(forceInit);
    }

    private int pushWriteLimit()
    {
        _writeLimit = _sharedMemory.getLong(SHARED_MQ_STRUCT_WRITE_LIMIT);
        while (true)
        {
            // check if the mq is full.
            if (isFull(_writeLimit, _sharedMemory.getLong(SHARED_MQ_STRUCT_READ_LIMIT)))
            {
                return ERROR_MQ_IS_FULL;
            }
            // push write limit.
            if (SHARED_MQ_REAL_LIMIT(_writeLimit) + SHARED_MQ_ENTRY_SIZE(_messageSize) >= _EOQ)
            {
                /**
                 * We have reached the end of the shared memory, try to flip to the beginning.
                 * If the flip is not successful, we get the latest write limit and retry the allocation.
                 */
                if (_sharedMemory.compareAndSwapLong(SHARED_MQ_STRUCT_WRITE_LIMIT, _writeLimit,
                        SHARED_MQ_FLIP_LIMIT(_writeLimit)))
                {
                    break;
                }
            }
            else if (_sharedMemory.compareAndSwapLong(SHARED_MQ_STRUCT_WRITE_LIMIT, _writeLimit,
                    _writeLimit + SHARED_MQ_ENTRY_SIZE(_messageSize)))
            {
                break;
            }
        }
        _writeLimit = SHARED_MQ_REAL_LIMIT(_writeLimit);
        _writeLimitPushed = true;
        return SUCCESS;
    }

    private int pushReadLimit()
    {
        _readLimit = _sharedMemory.getLong(SHARED_MQ_STRUCT_READ_LIMIT);
        while (true)
        {
            // check if the mq is empty.
            if (isEmpty(_readLimit, _sharedMemory.getLong(SHARED_MQ_STRUCT_WRITE_LIMIT)))
            {
                return ERROR_MQ_IS_EMPTY;
            }
            // push the read limit.
            if (SHARED_MQ_REAL_LIMIT(_readLimit) + SHARED_MQ_ENTRY_SIZE(_messageSize) >= _EOQ)
            {
                /**
                 * We have reached the end of the queue, try to flip to the beginning.
                 * If the flip is failed, it means that another reader has already
                 * finished the flip. Thus we have get the latest read limit and retry.
                 * If the flip is successful, we continue pushing the read limit.
                 */
                if (_sharedMemory.compareAndSwapLong(SHARED_MQ_STRUCT_READ_LIMIT, _readLimit,
                        SHARED_MQ_FLIP_LIMIT(_readLimit)))
                {
                    break;
                }
            }
            else if (_sharedMemory.compareAndSwapLong(SHARED_MQ_STRUCT_READ_LIMIT, _readLimit,
                    _readLimit + SHARED_MQ_ENTRY_SIZE(_messageSize)))
            {
                break;
            }
        }
        _readLimit = SHARED_MQ_REAL_LIMIT(_readLimit);
        _readLimitPushed = true;
        return SUCCESS;
    }

    private boolean isEmpty(long readLimit, long writeLimit)
    {
        /**
         * We do not have any limit that equals to _EOQ, so that
         * we do not deal with this case here.
         */
        if (SHARED_MQ_IS_FLIPPED(readLimit, writeLimit))
        {
            return false;
        }
        return SHARED_MQ_REAL_LIMIT(readLimit) >= SHARED_MQ_REAL_LIMIT(writeLimit);
    }

    private boolean isFull(long writeLimit, long readLimit)
    {
        /**
         * We do not have any limit that equals to _EOQ, so that
         * we do not deal with this case here.
         */
        if (SHARED_MQ_IS_FLIPPED(readLimit, writeLimit))
        {
            return SHARED_MQ_REAL_LIMIT(writeLimit) >= SHARED_MQ_REAL_LIMIT(readLimit);
        }
        return false;
    }

    public void init ()
    {
        init(false);
    }

    /**
     * This method initializes the message queue in shared memory.
     * It is reentrant.
     */
    public void init (boolean force)
    {
        if (force == true ||
                _sharedMemory.getByte(SHARED_MQ_STRUCT_INIT_FLAG) != SHARED_MQ_INITIALIZED)
        {
            _sharedMemory.clear();
            _sharedMemory.setLong(SHARED_MQ_STRUCT_READ_LIMIT, SHARED_MQ_STRUCT_DATA);
            _sharedMemory.setLong(SHARED_MQ_STRUCT_WRITE_LIMIT, SHARED_MQ_STRUCT_DATA);
            _sharedMemory.setByte(SHARED_MQ_STRUCT_INIT_FLAG, SHARED_MQ_INITIALIZED);
        }
    }

    /**
     * This method should be called before readMessage.
     * @return the error code or SUCCESS.
     */
    public int nextForRead ()
    {
        if (_readLimitPushed == false)
        {
            int ret = pushReadLimit();
            if (ret != SUCCESS)
            {
                return ret;
            }
        }
        byte writerStatus = _sharedMemory.getByte(_readLimit);
        if (writerStatus == TransactionStatus.COMMIT)
        {
            _readStartTime = 0;
            _sharedMemory.setByte(_readLimit, TransactionStatus.CLEAR);
            _sharedMemory.setByte(_readLimit + SHARED_MQ_LENGTH_MESSAGE_STATUS, TransactionStatus.RUNNING);
            return SUCCESS;
        }
        if (writerStatus == TransactionStatus.ROLLBACK)
        {
            _readStartTime = 0;
            _readLimitPushed = false;
            return ERROR_MQ_WRITER_IS_ROLLBACK;
        }
        // writerStatus == TransactionStatus::RUNNING
        if (_readStartTime == 0)
        {
            _readStartTime = System.currentTimeMillis();
            return ERROR_MQ_WRITER_IS_RUNNING;
        }
        else if (System.currentTimeMillis() - _readStartTime >= _timeout)
        {
            _sharedMemory.setByte(_readLimit, TransactionStatus.ROLLBACK);
            _readLimitPushed = false;
            _readStartTime = 0;
            return ERROR_MQ_WRITER_IS_ROLLBACK;
        }
        return ERROR_MQ_WRITER_IS_RUNNING;
    }
    /**
     * Read the message from message queue.
     * @param message the message to read to.
     */
    public void readMessage (Message message)
    {
        message.read(_sharedMemory, _readLimit + SHARED_MQ_LENGTH_MESSAGE_HEADER);
        _sharedMemory.setByte(_readLimit + SHARED_MQ_LENGTH_MESSAGE_STATUS, TransactionStatus.COMMIT);
        _readLimitPushed = false;
    }

    /**
     * This method should be called before writeMassage.
     * @return the error code or SUCCESS.
     */
    public int nextForWrite ()
    {
        if (_writeLimitPushed == false)
        {
            int ret = pushWriteLimit();
            if (ret != SUCCESS)
            {
                return ret;
            }
        }
        // check the status of the allocated entry.
        byte readerStatus = _sharedMemory.getByte(_writeLimit + SHARED_MQ_LENGTH_MESSAGE_STATUS);
        if (readerStatus == TransactionStatus.COMMIT ||
                readerStatus == TransactionStatus.ROLLBACK ||
                readerStatus == TransactionStatus.CLEAR)
        {
            _writeStartTime = 0;
            _sharedMemory.setByte(_writeLimit + SHARED_MQ_LENGTH_MESSAGE_STATUS, TransactionStatus.RUNNING);
            _sharedMemory.setByte(_writeLimit, TransactionStatus.RUNNING);
            return SUCCESS;
        }
        // readerStatus == TransactionStatus::RUNNING
        if (_writeStartTime == 0)
        {
            _writeStartTime = System.currentTimeMillis();
            return ERROR_MQ_READER_IS_RUNNING;
        }
        else if (System.currentTimeMillis() - _writeStartTime >= _timeout)
        {
            _sharedMemory.setByte(_writeLimit + SHARED_MQ_LENGTH_MESSAGE_STATUS,
                    TransactionStatus.ROLLBACK);
            _writeStartTime = 0;
            _sharedMemory.setByte(_writeLimit, TransactionStatus.RUNNING);
            return SUCCESS;
        }
        return ERROR_MQ_WRITER_IS_RUNNING;
    }
    /**
     * Write the given massage into message queue.
     * @param message the given message.
     */
    public void writeMessage (Message message)
    {
        message.write(_sharedMemory, _writeLimit + SHARED_MQ_LENGTH_MESSAGE_HEADER);
        _sharedMemory.setByte(_writeLimit, TransactionStatus.COMMIT);
        _writeLimitPushed = false;
    }

    /**
     * This method is only used for debugging.
     * @param message the message to print.
     */
    public void print (Message message)
    {
        System.out.println("------ Content in the Message Queue ------\n" +
                "read limit: (" +
                ((_sharedMemory.getLong(SHARED_MQ_STRUCT_READ_LIMIT) & SHARED_MQ_LIMIT_FLAG) > 0 ? "1" : "0") + ")" +
                SHARED_MQ_REAL_LIMIT(_sharedMemory.getLong(SHARED_MQ_STRUCT_READ_LIMIT)) + "\n" +
                "write limit: (" +
                ((_sharedMemory.getLong(SHARED_MQ_STRUCT_WRITE_LIMIT) & SHARED_MQ_LIMIT_FLAG) > 0 ? "1" : "0") + ")" +
                SHARED_MQ_REAL_LIMIT(_sharedMemory.getLong(SHARED_MQ_STRUCT_WRITE_LIMIT)) + "\n" +
                "init flag: " + _sharedMemory.getLong(SHARED_MQ_STRUCT_INIT_FLAG));
        for (long i = SHARED_MQ_STRUCT_DATA, j = 0; i < _EOQ; i += SHARED_MQ_ENTRY_SIZE(_messageSize), ++j)
        {
            System.out.println("message " + j + ": read status (" +
                    (int) _sharedMemory.getByte(i + SHARED_MQ_LENGTH_MESSAGE_STATUS) + "), write status (" +
                    (int) _sharedMemory.getByte(i) + "), value (" +
                    message.print(_sharedMemory, i + SHARED_MQ_LENGTH_MESSAGE_HEADER) + ")");
        }
    }

    public void close () throws Exception
    {
        this._sharedMemory.unmap();
    }
}
