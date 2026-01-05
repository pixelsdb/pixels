
/*
 * Copyright 2025 PixelsDB.
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
package io.pixelsdb.pixels.index.rockset;

import io.pixelsdb.pixels.common.index.ThreadId;
import io.pixelsdb.pixels.common.utils.ShutdownHookManager;
import io.pixelsdb.pixels.index.rockset.jni.RocksetReadOptions;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

final class RocksetThreadResources {
    private static final ThreadLocal<ThreadId> threadIds = ThreadLocal.withInitial(ThreadId::new);
    /**
     * Thread-local ReadOptions for each thread.
     */
    private static final Map<ThreadId, RocksetReadOptions> threadReadOptions = new ConcurrentHashMap<>();
    private static final Map<ThreadId, ByteBuffer> threadKeyBuffers = new ConcurrentHashMap<>();
    private static final Map<ThreadId, ByteBuffer> threadKeyBuffers2 = new ConcurrentHashMap<>();
    private static final Map<ThreadId, ByteBuffer> threadKeyBuffers3 = new ConcurrentHashMap<>();
    private static final Map<ThreadId, ByteBuffer> threadKeyBuffers4 = new ConcurrentHashMap<>();
    private static final Map<ThreadId, ByteBuffer> threadValueBuffers = new ConcurrentHashMap<>();

    private static final int DEFAULT_KEY_LENGTH = 32;
    private static final int VALUE_LENGTH = 8;

    static final ByteBuffer EMPTY_VALUE_BUFFER = ByteBuffer.allocateDirect(0);

    static {
        // Release resources when the process is shutting down.
        ShutdownHookManager.Instance().registerShutdownHook(
                RocksetThreadResources.class, false, RocksetThreadResources::release);
    }

    private RocksetThreadResources() { }

    /**
     * Get the current thread's ReadOptions.
     */
    static RocksetReadOptions getReadOptions() {
        ThreadId threadId = threadIds.get();
        RocksetReadOptions readOptions = threadReadOptions.get(threadId);
        if (readOptions == null) {
            readOptions = RocksetReadOptions.create();
            threadReadOptions.put(threadId, readOptions);
        }
        return readOptions;
    }

    /**
     * Get the current thread's key buffer.
     */
    static ByteBuffer getKeyBuffer(int length) {
        return internalGetKeyBuffer(threadKeyBuffers, length);
    }

    /**
     * Get the current thread's second key buffer.
     */
    static ByteBuffer getKeyBuffer2(int length) {
        return internalGetKeyBuffer(threadKeyBuffers2, length);
    }

    /**
     * Get the current thread's third key buffer.
     */
    static ByteBuffer getKeyBuffer3(int length) {
        return internalGetKeyBuffer(threadKeyBuffers3, length);
    }

    /**
     * Get the current thread's fourth key buffer.
     */
    static ByteBuffer getKeyBuffer4(int length) {
        return internalGetKeyBuffer(threadKeyBuffers4, length);
    }

    static ByteBuffer internalGetKeyBuffer(Map<ThreadId, ByteBuffer> keyBuffers, int length)
    {
        ThreadId threadId = threadIds.get();
        ByteBuffer keyBuffer = keyBuffers.get(threadId);
        // no need to add a lock as concurrent threads have unique thread ids
        if (keyBuffer == null)
        {
            keyBuffer = ByteBuffer.allocateDirect(Math.max(length, DEFAULT_KEY_LENGTH));
            keyBuffers.put(threadId, keyBuffer);
        }
        else if (keyBuffer.capacity() < length)
        {
            keyBuffer = ByteBuffer.allocateDirect(length);
            keyBuffers.put(threadId, keyBuffer);
        }
        keyBuffer.position(0);
        keyBuffer.limit(length);
        return keyBuffer;
    }

    /**
     * Get the current thread's value buffer.
     */
    static ByteBuffer getValueBuffer()
    {
        ThreadId threadId = threadIds.get();
        // no need to add a lock as concurrent threads have unique thread ids
        ByteBuffer valueBuffer = threadValueBuffers.computeIfAbsent(threadId,
                k -> ByteBuffer.allocateDirect(VALUE_LENGTH));
        valueBuffer.position(0);
        valueBuffer.limit(VALUE_LENGTH);
        return valueBuffer;
    }

    private static void release() {
        threadReadOptions.forEach((threadId, ctx) -> ctx.close());
        threadReadOptions.clear();
        threadValueBuffers.clear();
        threadKeyBuffers.clear();
        threadKeyBuffers2.clear();
        threadKeyBuffers3.clear();
        threadKeyBuffers4.clear();
    }
}


