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

/**
 * The interface of messages that can be write/read into/from
 * our message queue.
 * Created at: 3/19/21 (at DIAS EPFL)
 * Author: bian
 */
public interface Message
{
    /**
     * Return the footprint of this message in the shared memory.
     * @return the footprint in bytes.
     */
    int size ();

    /**
     * Set the message size. When reading a message from the message queue,
     * this method is called to set the message size for this.read().
     * @param messageSize
     */
    void ensureSize (int messageSize);

    /**
     * Read the content of this message from the given pos in sharedMemory.
     * @param mem the shared memory to read this message from.
     * @param pos the byte position in the shared memory.
     */
    void read (MemoryMappedFile mem, long pos);

    /**
     * Write the content of this message into the given pos in sharedMemory.
     * @param mem the shared memory to write this message into.
     * @param pos the byte position in the shared memory.
     */
    void write (MemoryMappedFile mem, long pos);

    /**
     * This method is only used for debugging.
     * @param mem the shared memory to read and print this message.
     * @param pos the byte position in the shared memory.
     * @return the content of the message.
     */
    String print (MemoryMappedFile mem, long pos);
}
