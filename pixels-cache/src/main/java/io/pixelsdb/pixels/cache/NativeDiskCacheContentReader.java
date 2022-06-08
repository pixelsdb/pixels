package io.pixelsdb.pixels.cache;

import com.google.protobuf.Descriptors;
import sun.nio.ch.DirectBuffer;

import java.io.FileDescriptor;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;

public class NativeDiskCacheContentReader implements CacheContentReader {
    static {
//        System.loadLibrary("DiskCacheContentReader");
    }

    private final RandomAccessFile content;
    private final FileDescriptor fileDescriptor;
    private final int fd;

    NativeDiskCacheContentReader(String loc) throws IOException, NoSuchFieldException, IllegalAccessException {
        this.content = new RandomAccessFile(loc, "r");
        this.content.seek(0);
        fileDescriptor = this.content.getFD();
        Field fdField = FileDescriptor.class.getDeclaredField("fd");
        fdField.setAccessible(true);
        fd = fdField.getInt(fileDescriptor);
    }

    @Override
    public void read(PixelsCacheIdx idx, byte[] buf) throws IOException {
        read(fd, idx.offset, idx.length, buf);
    }

    private native void read(int fd, long offset, int length, byte[] buf);

    @Override
    public void batchRead(PixelsCacheIdx[] idxs, ByteBuffer buf) throws IOException {
        throw new IOException("Not Implemented");
    }
}
