/*
 * This file is copied from MappedBus:
 *
 *   Copyright 2015 Caplogic AB.
 *   Licensed under the Apache License, Version 2.0.
 */
package io.pixelsdb.pixels.cache.mq;

import io.pixelsdb.pixels.cache.MemoryMappedFile;

import java.io.EOFException;
import java.io.IOException;

/**
 * Class for reading messages from the bus.
 * <p>
 * Messages can either be message based or byte array based.
 * <p>
 * The typical usage is as follows:
 * <pre>
 * {@code
 * // Construct a reader
 * MappedBusReader reader = new MappedBusReader("/tmp/test", 100000L, 32);
 * reader.open();
 *
 * // A: read messages as objects
 * while (true) {
 *    if (reader.next()) {
 *       int type = reader.readType();
 *       if (type == 0) {
 *          reader.readMessage(priceUpdate)
 *       }
 *    }
 * }
 *
 * // B: read messages as byte arrays
 * while (true) {
 *    if (reader.next()) {
 *       int length = reader.read(buffer, 0);
 *    }
 * }
 *
 * // Close the reader
 * reader.close();
 * }
 * </pre>
 */
public class MappedBusReader
{

    protected static final long MAX_TIMEOUT_COUNT = 100;

    private final String fileName;

    private final long fileSize;

    private final int recordSize;

    private MemoryMappedFile mem;

    private long limit = MappedBusConstants.Structure.Data;

    private long initialLimit;

    private int maxTimeout = 2000;

    protected long timerStart;

    protected long timeoutCounter;

    private boolean typeRead;

    /**
     * Constructs a new reader.
     *
     * @param fileName   the name of the memory mapped file
     * @param fileSize   the maximum size of the file
     * @param recordSize the maximum size of a record (excluding status flags and meta data)
     */
    public MappedBusReader(String fileName, long fileSize, int recordSize)
    {
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.recordSize = recordSize;
    }

    /**
     * Opens the reader.
     *
     * @throws IOException if there was a problem opening the file
     */
    public void open()
            throws IOException
    {
        try
        {
            mem = new MemoryMappedFile(fileName, fileSize);
        }
        catch (Exception e)
        {
            throw new IOException("Unable to open the file: " + fileName, e);
        }
        initialLimit = mem.getLongVolatile(MappedBusConstants.Structure.Limit);
    }

    /**
     * Sets the time for a reader to wait for a record to be committed.
     * <p>
     * When the timeout occurs the reader will mark the record as "rolled back" and
     * the record is ignored.
     *
     * @param timeout the timeout in milliseconds
     */
    public void setTimeout(int timeout)
    {
        this.maxTimeout = timeout;
    }

    /**
     * Steps forward to the next record if there's one available.
     * <p>
     * The method has a timeout for how long it will wait for the commit field to be set. When the timeout is
     * reached it will set the roll back field and skip over the record.
     *
     * @return true, if there's a new record available, otherwise false
     * @throws EOFException in case the end of the file was reached
     */
    public boolean next()
            throws EOFException
    {
        if (limit >= fileSize)
        {
            throw new EOFException("End of file was reached");
        }
        if (mem.getLongVolatile(MappedBusConstants.Structure.Limit) <= limit)
        {
            return false;
        }
        byte commit = mem.getByteVolatile(limit);
        byte rollback = mem.getByteVolatile(limit + MappedBusConstants.Length.Commit);
        if (rollback == MappedBusConstants.Rollback.Set)
        {
            limit += MappedBusConstants.Length.RecordHeader + recordSize;
            timeoutCounter = 0;
            timerStart = 0;
            return false;
        }
        if (commit == MappedBusConstants.Commit.Set)
        {
            timeoutCounter = 0;
            timerStart = 0;
            return true;
        }
        timeoutCounter++;
        if (timeoutCounter >= MAX_TIMEOUT_COUNT)
        {
            if (timerStart == 0)
            {
                timerStart = System.currentTimeMillis();
            }
            else
            {
                if (System.currentTimeMillis() - timerStart >= maxTimeout)
                {
                    mem.putByteVolatile(limit + MappedBusConstants.Length.Commit, MappedBusConstants.Rollback.Set);
                    limit += MappedBusConstants.Length.RecordHeader + recordSize;
                    timeoutCounter = 0;
                    timerStart = 0;
                    return false;
                }
            }
        }
        return false;
    }

    /**
     * Reads the message type.
     *
     * @return the message type
     */
    public int readType()
    {
        typeRead = true;
        limit += MappedBusConstants.Length.StatusFlags;
        int type = mem.getInt(limit);
        limit += MappedBusConstants.Length.Metadata;
        return type;
    }

    /**
     * Reads the next message.
     *
     * @param message the message object to populate
     * @return the message object
     */
    public MappedBusMessage readMessage(MappedBusMessage message)
    {
        if (!typeRead)
        {
            readType();
        }
        typeRead = false;
        message.read(mem, limit);
        limit += recordSize;
        return message;
    }

    /**
     * Reads the next buffer of data.
     *
     * @param dst    the input buffer
     * @param offset the offset in the buffer of the first byte to read data into
     * @return the length of the record that was read
     */
    public int readBuffer(byte[] dst, int offset)
    {
        limit += MappedBusConstants.Length.StatusFlags;
        int length = mem.getInt(limit);
        limit += MappedBusConstants.Length.Metadata;
        mem.getBytes(limit, dst, offset, length);
        limit += recordSize;
        return length;
    }

    /**
     * Indicates whether all records available when the reader was created have been read.
     *
     * @return true, if all records available from the start was read, otherwise false
     */
    public boolean hasRecovered()
    {
        return limit >= initialLimit;
    }

    /**
     * Closes the reader.
     *
     * @throws IOException if there was an error closing the file
     */
    public void close()
            throws IOException
    {
        try
        {
            mem.unmap();
        }
        catch (Exception e)
        {
            throw new IOException("Unable to close the file", e);
        }
    }
}