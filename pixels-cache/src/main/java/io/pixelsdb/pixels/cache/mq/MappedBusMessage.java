/*
 * This file is copied from MappedBus:
 *
 *   Copyright 2015 Caplogic AB.
 *   Licensed under the Apache License, Version 2.0.
 */
package io.pixelsdb.pixels.cache.mq;

import io.pixelsdb.pixels.cache.MemoryMappedFile;

/**
 * Interface for messages that can be serialized to the bus.
 */
public interface MappedBusMessage
{

    /**
     * Writes a message to the bus.
     *
     * @param mem an instance of the memory mapped file
     * @param pos the start of the current record
     */
    public void write(MemoryMappedFile mem, long pos);

    /**
     * Reads a message from the bus.
     *
     * @param mem an instance of the memory mapped file
     * @param pos the start of the current record
     */
    public void read(MemoryMappedFile mem, long pos);

    /**
     * Returns the message type.
     *
     * @return the message type
     */
    public int type();
}