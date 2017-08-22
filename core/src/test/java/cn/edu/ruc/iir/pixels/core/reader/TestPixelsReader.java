package cn.edu.ruc.iir.pixels.core.reader;

import cn.edu.ruc.iir.pixels.core.PixelsProto;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;

/**
 * pixels
 *
 * @author guodong
 */
public class TestPixelsReader
{
    @Test
    public void validateWriter()
    {
        String filePath = "hdfs://192.168.124.15:9000/test_big3.pxl";
        Path path = new Path(filePath);

        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        try {
            FileSystem fs = FileSystem.get(URI.create(filePath), conf);
            FSDataInputStream inStream = fs.open(path);
            long length = fs.getFileStatus(path).getLen();
            inStream.seek(length - 8);
            long pos = inStream.readLong();
            System.out.println("File length: " + pos);
            inStream.seek(pos - 4);
            int tailLen = inStream.readInt();
            System.out.println("Tail length: " + tailLen);
            long tailOffset = length - 8 - 4 - tailLen;
            inStream.seek(tailOffset);
            ByteBuffer tailBuffer = ByteBuffer.allocate(tailLen);
            inStream.read(tailBuffer);

            PixelsProto.FileTail fileTail =
                    PixelsProto.FileTail.parseFrom(tailBuffer.array());
            System.out.println("=========== FILE TAIL ===========");
            System.out.println(fileTail);

            PixelsProto.Footer footer = fileTail.getFooter();
            PixelsProto.RowGroupInformation rowGroupInfo = footer.getRowGroupInfos(0);
            int rowGroupOffset = (int) rowGroupInfo.getOffset();
            int rowGroupDataLen = (int) rowGroupInfo.getDataLength();
            int rowGroupFooterLen = (int) rowGroupInfo.getFooterLength();

            ByteBuffer rowGroupFooterBuffer = ByteBuffer.allocate(rowGroupFooterLen);
            inStream.seek(rowGroupOffset + rowGroupDataLen);
            inStream.read(rowGroupFooterBuffer);
            PixelsProto.RowGroupFooter rowGroupFooter =
                    PixelsProto.RowGroupFooter.parseFrom(rowGroupFooterBuffer.array());
            System.out.println("========== ROW GROUP ===========");
            System.out.println(rowGroupFooter);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}
