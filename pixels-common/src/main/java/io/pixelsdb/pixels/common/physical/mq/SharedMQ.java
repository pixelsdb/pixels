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
package io.pixelsdb.pixels.common.physical.mq;

import io.pixelsdb.pixels.common.physical.natives.MemoryMappedFile;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkArgument;
import static io.pixelsdb.pixels.common.error.ErrorCode.*;
import static java.util.Objects.requireNonNull;

/**
 * The message queue that can be used to collect cache miss messages,
 * or for low-latency interprocess communications on the same node.
 * It should be backed by an in-memory file for high performance.
 *
 * <p>The maximum message size supported by this message queue is 2GB.</p>
 *
 * @author hank
 * @create 2021-03-19 (at DIAS EPFL)
 */
public class SharedMQ
{
    public static class TransStatus
    {
        public static final byte CLEAR = 0;
        public static final byte RUNNING = 1;
        public static final byte ROLLBACK = 2;
        public static final byte COMMIT = 3;
    }

    public static final long SHARED_MQ_STRUCT_READ_LIMIT = 0L;
    public static final long SHARED_MQ_STRUCT_WRITE_LIMIT = 8L;
    public static final long SHARED_MQ_STRUCT_INIT_FLAG = 16L;
    // although we only use 17 bytes are used for metadata, we use 24 bytes for word alignment.
    public static final long SHARED_MQ_STRUCT_DATA = 24L;
    public static final long SHARED_MQ_LIMIT_FLAG = 0x4000000000000000L;
    public static final long SHARED_MQ_LIMIT_MASK = 0x3FFFFFFFFFFFFFFFL;
    /**
     * 2 bytes for eht read and write status flag, and 4 bytes for the message size.
     * Thus, the largest message can be 2GB.
     */
    public static final int SHARED_MQ_LENGTH_MESSAGE_HEADER = 2 + 4;
    public static final int SHARED_MQ_LENGTH_MESSAGE_STATUSES = 2;
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

    private final MemoryMappedFile _sharedMemory;
    //private int _messageSize;
    private final int _timeout;
    private boolean _readLimitPushed;
    private boolean _writeLimitPushed;
    /**
     * The reader's head of the queue, i.e., the start offset of the next
     * unread message in the shared memory.
     */
    private long _readLimit;
    /**
     * The writer's head of the queue, i.e., the start offset for the next
     * message to be written in the shared memory.
     */
    private long _writeLimit;
    private long _readStartTime;
    private long _writeStartTime;
    /**
     * The end of the queue. It is also the length of the shared memory
     * used by this message queue.
     */
    private final long _EOQ;

    public SharedMQ (String location, long totalSize) throws IOException
    {
        this(location, totalSize, SHARED_MQ_DEFAULT_TIMEOUT_MS, false);
    }

    public SharedMQ (String location, long totalSize, boolean forceInit) throws IOException
    {
        this(location, totalSize, SHARED_MQ_DEFAULT_TIMEOUT_MS, forceInit);
    }

    public SharedMQ (String location, long totalSize, int timeout, boolean forceInit) throws IOException
    {
        _timeout = timeout;
        //_messageSize = messageSize;
        _readLimitPushed = false;
        _writeLimitPushed = false;
        _readLimit = SHARED_MQ_STRUCT_DATA;
        _writeLimit = SHARED_MQ_STRUCT_DATA;
        _readStartTime = 0;
        _writeStartTime = 0;
        _sharedMemory = new MemoryMappedFile(location, totalSize);
        _EOQ = _sharedMemory.getSize();
        init(forceInit);
    }

    private int pushWriteLimit(int messageSize)
    {
        while (true)
        {
            _writeLimit = _sharedMemory.getLong(SHARED_MQ_STRUCT_WRITE_LIMIT);
            // check if the mq is full.
            if (isFull(_writeLimit, _sharedMemory.getLong(SHARED_MQ_STRUCT_READ_LIMIT)))
            {
                return ERROR_MQ_IS_FULL;
            }
            // push write limit.
            if (SHARED_MQ_REAL_LIMIT(_writeLimit) + SHARED_MQ_ENTRY_SIZE(messageSize) >= _EOQ)
            {
                /**
                 * We have reached the end of the shared memory, try to flip to the beginning.
                 * If the flip is not successful, we get the latest write limit and retry the allocation.
                 *
                 * Before flipping, we set the flip flag (message size == 0) for readers if the
                 * remaining bytes are more than SHARED_MQ_LENGTH_MESSAGE_HEADER.
                 */
                if (SHARED_MQ_REAL_LIMIT(_writeLimit) + SHARED_MQ_LENGTH_MESSAGE_HEADER < _EOQ)
                {
                    _sharedMemory.setInt(
                            SHARED_MQ_REAL_LIMIT(_writeLimit) + SHARED_MQ_LENGTH_MESSAGE_STATUSES, 0);
                }
                if (_sharedMemory.compareAndSwapLong(SHARED_MQ_STRUCT_WRITE_LIMIT, _writeLimit,
                        SHARED_MQ_FLIP_LIMIT(_writeLimit)))
                {
                    _writeLimit = SHARED_MQ_FLIP_LIMIT(_writeLimit);
                }
                else
                {
                    continue;
                }
            }

            // push the write limit.
            if (_sharedMemory.compareAndSwapLong(SHARED_MQ_STRUCT_WRITE_LIMIT, _writeLimit,
                    _writeLimit + SHARED_MQ_ENTRY_SIZE(messageSize)))
            {
                break;
            }
        }
        _writeLimit = SHARED_MQ_REAL_LIMIT(_writeLimit);
        _writeLimitPushed = true;
        return SUCCESS;
    }

    private int pushReadLimit(Message message)
    {
        int messageSize = 0;

        while (true)
        {
            _readLimit = _sharedMemory.getLong(SHARED_MQ_STRUCT_READ_LIMIT);
            // check if the mq is empty.
            if (isEmpty(_readLimit, _sharedMemory.getLong(SHARED_MQ_STRUCT_WRITE_LIMIT)))
            {
                return ERROR_MQ_IS_EMPTY;
            }
            // push the read limit.
            boolean flip = false;

            if (SHARED_MQ_REAL_LIMIT(_readLimit) + SHARED_MQ_LENGTH_MESSAGE_HEADER < _EOQ)
            {
                messageSize = _sharedMemory.getInt(SHARED_MQ_REAL_LIMIT(_readLimit) +
                        SHARED_MQ_LENGTH_MESSAGE_STATUSES);
                if (messageSize < 0)
                {
                    return ERROR_MQ_READER_INVALID_MESSAGE_LENGTH;
                }
                else if (messageSize == 0)
                {
                    /**
                     * messageSize == 0 means the reader should flip to read the next message.
                     * Writer will set this value if the last remaining bytes before EOQ is more
                     * than SHARED_MQ_LENGTH_MESSAGE_HEADER.
                     */
                    flip = true;
                }
            }
            else
            {
                // If the remaining bytes is less than SHARED_MQ_LENGTH_MESSAGE_HEADER, flip.
                flip = true;
            }
            if (flip)
            {
                /**
                 * We have reached the end of the queue, try to flip to the beginning.
                 * If the flip is failed, it means that another reader has already
                 * finished the flip. In this case, we get the latest read limit and retry.
                 * If the flip is successful, we continue pushing the read limit.
                 */
                if (_sharedMemory.compareAndSwapLong(SHARED_MQ_STRUCT_READ_LIMIT, _readLimit,
                        SHARED_MQ_FLIP_LIMIT(_readLimit)))
                {
                    /**
                     * flip success, use the flipped read limit to get the real message size
                     * and continue pushing.
                     */
                    _readLimit = SHARED_MQ_FLIP_LIMIT(_readLimit);
                    messageSize = _sharedMemory.getInt(SHARED_MQ_REAL_LIMIT(_readLimit) +
                            SHARED_MQ_LENGTH_MESSAGE_STATUSES);
                }
                else
                {
                    continue;
                }
            }
            // push the read limit.
            if (_sharedMemory.compareAndSwapLong(SHARED_MQ_STRUCT_READ_LIMIT, _readLimit,
                    _readLimit + SHARED_MQ_ENTRY_SIZE(messageSize)))
            {
                break;
            }
        }
        message.ensureSize(messageSize);
        _readLimit = SHARED_MQ_REAL_LIMIT(_readLimit);
        _readLimitPushed = true;
        return SUCCESS;
    }

    private boolean isEmpty(long readLimit, long writeLimit)
    {
        /**
         * Read or write limit never equals to _EOQ due to flipping, so that
         * we do not deal with this case here.
         */
        if (SHARED_MQ_IS_FLIPPED(readLimit, writeLimit))
        {
            // The queue is not empty if it is flipped.
            return false;
        }
        return SHARED_MQ_REAL_LIMIT(readLimit) >= SHARED_MQ_REAL_LIMIT(writeLimit);
    }

    private boolean isFull(long writeLimit, long readLimit)
    {
        /**
         * Read or write limit never equals to _EOQ due to flipping, so that
         * we do not deal with this case here.
         */
        if (SHARED_MQ_IS_FLIPPED(readLimit, writeLimit))
        {
            return SHARED_MQ_REAL_LIMIT(writeLimit) >= SHARED_MQ_REAL_LIMIT(readLimit);
        }
        // If the queue is not flipped, it is not full.
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
        if (force || _sharedMemory.getByte(SHARED_MQ_STRUCT_INIT_FLAG) != SHARED_MQ_INITIALIZED)
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
    public int nextForRead (Message message)
    {
        requireNonNull(message, "message is null");
        if (!_readLimitPushed)
        {
            int ret = pushReadLimit(message);
            if (ret != SUCCESS)
            {
                return ret;
            }
        }
        byte writerStatus = _sharedMemory.getByte(_readLimit);
        if (writerStatus == TransStatus.COMMIT)
        {
            _readStartTime = 0;
            _sharedMemory.setByte(_readLimit, TransStatus.CLEAR);
            _sharedMemory.setByte(_readLimit + SHARED_MQ_LENGTH_MESSAGE_STATUS, TransStatus.RUNNING);
            return SUCCESS;
        }
        if (writerStatus == TransStatus.ROLLBACK)
        {
            _readStartTime = 0;
            _readLimitPushed = false;
            return ERROR_MQ_WRITER_IS_ROLLBACK;
        }
        // writerStatus == TransStatus::RUNNING
        if (_readStartTime == 0)
        {
            _readStartTime = System.currentTimeMillis();
            return ERROR_MQ_WRITER_IS_RUNNING;
        }
        else if (System.currentTimeMillis() - _readStartTime >= _timeout)
        {
            _sharedMemory.setByte(_readLimit, TransStatus.ROLLBACK);
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
        requireNonNull(message, "message is null");
        message.read(_sharedMemory, _readLimit + SHARED_MQ_LENGTH_MESSAGE_HEADER);
        _sharedMemory.setByte(_readLimit + SHARED_MQ_LENGTH_MESSAGE_STATUS, TransStatus.COMMIT);
        _readLimitPushed = false;
    }

    /**
     * This method should be called before writeMassage.
     * @return the error code or SUCCESS.
     */
    public int nextForWrite (int messageSize)
    {
        checkArgument(messageSize > 0, "messageSize must be positive");
        if (!_writeLimitPushed)
        {
            int ret = pushWriteLimit(messageSize);
            if (ret != SUCCESS)
            {
                return ret;
            }
        }
        // check the status of the allocated entry.
        byte readerStatus = _sharedMemory.getByte(_writeLimit + SHARED_MQ_LENGTH_MESSAGE_STATUS);
        if (readerStatus == TransStatus.COMMIT ||
                readerStatus == TransStatus.ROLLBACK ||
                readerStatus == TransStatus.CLEAR)
        {
            _writeStartTime = 0;
            _sharedMemory.setByte(_writeLimit + SHARED_MQ_LENGTH_MESSAGE_STATUS, TransStatus.RUNNING);
            _sharedMemory.setByte(_writeLimit, TransStatus.RUNNING);
            return SUCCESS;
        }
        // readerStatus == TransStatus::RUNNING
        if (_writeStartTime == 0)
        {
            _writeStartTime = System.currentTimeMillis();
            return ERROR_MQ_READER_IS_RUNNING;
        }
        else if (System.currentTimeMillis() - _writeStartTime >= _timeout)
        {
            _sharedMemory.setByte(_writeLimit + SHARED_MQ_LENGTH_MESSAGE_STATUS,
                    TransStatus.ROLLBACK);
            _writeStartTime = 0;
            _sharedMemory.setByte(_writeLimit, TransStatus.RUNNING);
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
        requireNonNull(message, "message is null");
        message.write(_sharedMemory, _writeLimit + SHARED_MQ_LENGTH_MESSAGE_HEADER);
        _sharedMemory.setByte(_writeLimit, TransStatus.COMMIT);
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
        int messageSize = 0;
        for (long i = SHARED_MQ_STRUCT_DATA, j = 0; i < _EOQ; i += SHARED_MQ_ENTRY_SIZE(messageSize), ++j)
        {
            System.out.println("message " + j + ": read status (" +
                    (int) _sharedMemory.getByte(i + SHARED_MQ_LENGTH_MESSAGE_STATUS) + "), write status (" +
                    (int) _sharedMemory.getByte(i) + "), value (" +
                    message.print(_sharedMemory, i + SHARED_MQ_LENGTH_MESSAGE_HEADER) + ")");
            messageSize = _sharedMemory.getInt(i + SHARED_MQ_LENGTH_MESSAGE_STATUSES);
            if (messageSize <= 0)
            {
                System.err.println("messageSize must be positive.");
                break;
            }
        }
    }

    public void close () throws Exception
    {
        this._sharedMemory.unmap();
    }
}
