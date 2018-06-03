package cn.edu.ruc.iir.pixels.core.compactor;

import cn.edu.ruc.iir.pixels.core.TestParams;
import cn.edu.ruc.iir.pixels.core.TypeDescription;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

public class TestPixelsCompactor
{
    @Test
    public void test ()
    {
        String filePath = TestParams.filePath;
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        try {
            FileSystem fs = FileSystem.get(URI.create(filePath), conf);
            TypeDescription schema = TypeDescription.fromString(TestParams.schemaStr);

            CompactLayout layout = new CompactLayout(3, 2);
            layout.addColumnlet(0, 1);
            layout.addColumnlet(1, 1);
            layout.addColumnlet(0, 0);
            layout.addColumnlet(1, 0);
            layout.addColumnlet(2, 1);
            layout.addColumnlet(2, 0);

            PixelsCompactor pixelsCompactor =
                    PixelsCompactor.newBuilder()
                            .setSchema(schema)
                            .setSourcePaths(TestParams.sourcePaths)
                            .setCompactLayout(layout)
                            .setFS(fs)
                            .setFilePath(new Path(filePath))
                            .setBlockSize(1024*1024*1024)
                            .setReplication((short) 1)
                            .setBlockPadding(false)
                            .build();

            pixelsCompactor.compact();
            pixelsCompactor.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}
