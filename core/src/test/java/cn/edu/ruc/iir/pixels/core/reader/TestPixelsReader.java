package cn.edu.ruc.iir.pixels.core.reader;

import cn.edu.ruc.iir.pixels.core.PixelsProto;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;

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
        String filePath = "hdfs://127.0.0.1:9000/test2.pxl";
        String metaPath = System.getProperty("user.home") + "/meta";
        Path path = new Path(filePath);

        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        try {
            BufferedWriter metaWriter = new BufferedWriter(new FileWriter(metaPath, false));
            FileSystem fs = FileSystem.get(URI.create(filePath), conf);
            FSDataInputStream inStream = fs.open(path);
            long length = fs.getFileStatus(path).getLen();
            inStream.seek(length - 8);
            long pos = inStream.readLong();
            metaWriter.write("File length: " + pos + "\n");
            inStream.seek(pos - 4);
            int tailLen = inStream.readInt();
            metaWriter.write("Tail length: " + tailLen + "\n");
            long tailOffset = length - 8 - 4 - tailLen;
            inStream.seek(tailOffset);
            byte[] tailBuffer = new byte[tailLen];
            inStream.read(tailBuffer);

            PixelsProto.FileTail fileTail =
                    PixelsProto.FileTail.parseFrom(tailBuffer);
            metaWriter.write("=========== FILE TAIL ===========\n");
            metaWriter.write(fileTail.toString() + "\n");

            PixelsProto.Footer footer = fileTail.getFooter();
            for (int i = 0; i < footer.getRowGroupInfosCount(); i++) {
                PixelsProto.RowGroupInformation rowGroupInfo = footer.getRowGroupInfos(i);
                int rowGroupFooterOffset = (int) rowGroupInfo.getFooterOffset();
                int rowGroupFooterLen = (int) rowGroupInfo.getFooterLength();
                byte[] rowGroupFooterBuffer = new byte[rowGroupFooterLen];
                inStream.seek(rowGroupFooterOffset);
                inStream.readFully(rowGroupFooterBuffer);
                PixelsProto.RowGroupFooter rowGroupFooter =
                        PixelsProto.RowGroupFooter.parseFrom(rowGroupFooterBuffer);
                metaWriter.write("========== ROW GROUP " +  i + " ===========\n");
                metaWriter.write(rowGroupFooter.toString() + "\n");
            }
            metaWriter.flush();
            metaWriter.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}
