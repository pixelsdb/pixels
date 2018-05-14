package cn.edu.ruc.iir.pixels.core;

import cn.edu.ruc.iir.pixels.core.reader.PixelsReaderOption;
import cn.edu.ruc.iir.pixels.core.reader.PixelsRecordReader;
import cn.edu.ruc.iir.pixels.core.vector.VectorizedRowBatch;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * pixels
 *
 * @author guodong
 */
public class MockPixelsReader
        implements Runnable
{
    private final FileSystem fs;
    private final Path filePath;
    private final String[] schema;

    public MockPixelsReader(FileSystem fs, Path filePath, String[] schema)
    {
        this.fs = fs;
        this.filePath = filePath;
        this.schema = schema;
    }

    @Override
    public void run()
    {
        PixelsReaderOption option = new PixelsReaderOption();
        option.skipCorruptRecords(true);
        option.tolerantSchemaEvolution(true);
        option.includeCols(schema);

        try {
            PixelsReader pixelsReader = PixelsReaderImpl.newBuilder()
                    .setFS(fs)
                    .setPath(filePath)
                    .build();
            PixelsRecordReader recordReader = pixelsReader.read(option);
            VectorizedRowBatch rowBatch;
            int batchSize = 10000;
            long num = 0;
            long start = System.currentTimeMillis();
            while (true) {
                rowBatch = recordReader.readBatch(batchSize);
                if (rowBatch.endOfFile) {
                    num += rowBatch.size;
                    break;
                }
                num += rowBatch.size;
            }
            long end = System.currentTimeMillis();
            System.out.println("[" + filePath.getName() + "] "
                    + start + " " + end + " " + num + ", cpu cost: " + (end - start));
            pixelsReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
