package io.pixelsdb.pixels.cache;

import org.junit.Test;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class TestHashIndexReader {

    @Test
    public void testConstructor() {
        try {
            MemoryMappedFile indexFile = new MemoryMappedFile("/dev/shm/pixels.hash-index", 102400000);
            HashIndexReader reader = new HashIndexReader(indexFile);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testDiskIndexReader() {
        try {
            MemoryMappedFile indexFile = new MemoryMappedFile("/dev/shm/pixels.hash-index", 102400000);
            RandomAccessFile indexDiskFile = new RandomAccessFile("/dev/shm/pixels.hash-index", "r");

            // read some chars
            byte[] buf = new byte[5];
            ByteBuffer wrapBuf = ByteBuffer.wrap(buf);
            indexDiskFile.seek(0);
            indexDiskFile.read(buf);
            Charset utf8 = StandardCharsets.UTF_8;
            System.out.println(utf8.decode(wrapBuf));
            indexFile.getBytes(0, buf, 0, 5);
            wrapBuf.position(0);
            System.out.println(utf8.decode(wrapBuf));

            // print the byte value
            for(int i = 0; i < buf.length; ++i) {
                System.out.println(buf[i]);
            }

            // try to read the int
            // Note: the RandomAccessFile read everything from big-endian
            System.out.println("mmap " + indexFile.getInt(0));
            indexDiskFile.seek(0);
            System.out.println("disk " + indexDiskFile.readInt());

            System.out.println("mmap " + indexFile.getLong(0));
            indexDiskFile.seek(0);
            System.out.println("disk " + indexDiskFile.readLong());

            HashIndexReader reader = new HashIndexReader(indexFile);
            HashIndexDiskReader diskReader = new HashIndexDiskReader(indexDiskFile);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Test
    public void testNativeSearch() {
        try {
            MemoryMappedFile indexFile = new MemoryMappedFile("/dev/shm/pixels.hash-index", 102400000);
            HashIndexReader reader = new HashIndexReader(indexFile);
            // blk=1073747693, rg=12, col=518
            System.out.println(reader.search(1073747693L, (short) 12, (short) 518));
            System.out.println(reader.nativeSearch(1073747693L, (short) 12, (short) 518));
            System.out.println(reader.nativeSearch(1073747647L, (short) 27, (short) 694));
            System.out.println(reader.nativeSearch(1073747600L, (short) 10, (short) 1013));


        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
