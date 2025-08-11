/*
 * Copyright 2023 PixelsDB.
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
package io.pixelsdb.pixels.common.physical;

import com.sun.jna.Native;
import com.sun.jna.Platform;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.PointerByReference;
import org.junit.Test;
import sun.nio.ch.DirectBuffer;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;

/**
 * @author hank
 * @create 2023-02-02
 */
public class TestJNA
{
    private static native long pread(int fd, Pointer buf, long count, long offset);
    private static native int open(String pathname, int flags);
    public native int close(int fd); // musn't forget to do this
    private static native String strerror(int errnum);
    private static native int posix_memalign(PointerByReference memptr, long alignment, long size);
    private static native void free(long ptr);

    public static final int O_RDONLY = 00;
    public static final int O_WRONLY = 01;
    public static final int O_RDWR = 02;
    public static final int O_CREAT = 0100;
    public static final int O_TRUNC = 01000;
    public static final int O_DIRECT = 040000;
    public static final int O_SYNC = 04000000;

    private static final int BLOCK_SIZE = 4096;

    static
    {
        Native.register(Platform.C_LIBRARY_NAME);
    }

    private ByteBuffer readDirect(int fd, long fileOffset, int length)
    {
        int toAllocate = (length/BLOCK_SIZE*BLOCK_SIZE) + (BLOCK_SIZE*3);
        long startOffset = fileOffset / BLOCK_SIZE * BLOCK_SIZE;
        long endOffset = (fileOffset+length) / BLOCK_SIZE * BLOCK_SIZE + BLOCK_SIZE;
        int fileAlign = (int) (fileOffset % BLOCK_SIZE);
        int toRead = (int) (endOffset - startOffset);
        ByteBuffer buffer = ByteBuffer.allocateDirect(toAllocate);
        long addr = ((DirectBuffer) buffer).address();
        int bufferAlign = BLOCK_SIZE - (int) (addr % BLOCK_SIZE);
        buffer.position(fileAlign + bufferAlign);
        buffer.limit(buffer.position()+length);
        long read = pread(fd, Pointer.createConstant(addr + bufferAlign), toRead, fileOffset-fileAlign);
        System.out.println(read);
        return buffer;
    }

    @Test
    public void testDirect()
    {
        int fd = open("/home/hank/20230126155625_0.pxl", O_DIRECT | O_RDONLY);
        ByteBuffer buffer = readDirect(fd, 4094, 8);
        for (int i = 0; i < 8; ++i)
        {
            System.out.println(buffer.get());
        }
        close(fd);
        System.out.println(strerror(Native.getLastError()));
    }

    @Test
    public void testNonDirect()
    {
        int fd = open("/home/hank/20230126155625_0.pxl", O_RDONLY);
        ByteBuffer buffer = ByteBuffer.allocateDirect(4096);
        long addr = ((DirectBuffer) buffer).address();
        long read = pread(fd, Pointer.createConstant(addr), 4096, 4094);
        System.out.println(read);
        for (int i = 0; i < 8; ++i)
        {
            System.out.println(buffer.get());
        }
        close(fd);
        System.out.println(strerror(Native.getLastError()));
    }

    @Test
    public void testAllocate() throws ClassNotFoundException, NoSuchFieldException, IllegalAccessException
    {
        Field peer = Class.forName("com.sun.jna.Pointer").getDeclaredField("peer");
        peer.setAccessible(true);
        PointerByReference pointerToPointer = new PointerByReference();
        posix_memalign(pointerToPointer, 4096, 8192);
        free((Long) peer.get(pointerToPointer.getValue()));
    }
}
