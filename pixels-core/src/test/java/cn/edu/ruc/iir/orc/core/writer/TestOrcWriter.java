package cn.edu.ruc.iir.orc.core.writer;

import cn.edu.ruc.iir.pixels.core.TestParams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.junit.Test;

import java.io.IOException;
import java.sql.Timestamp;

import static org.apache.orc.CompressionKind.NONE;

public class TestOrcWriter {

    String orcPath = "hdfs://dbiir10:9000/pixels/pixels/test_105/11.orc";

    @Test
    public void testWriterAll() {
        Configuration conf = new Configuration();
        TypeDescription schema = TypeDescription.fromString(TestParams.schemaStr);
        Writer writer = null;
        try
        {
            writer = OrcFile.createWriter(new Path(orcPath),
                    OrcFile.writerOptions(conf)
                            .blockSize(TestParams.blockSize)
                            .blockPadding(true)
                            .stripeSize(64)
                            .rowIndexStride(TestParams.pixelStride)
                            .setSchema(schema)
                            .compress(NONE)
                            .setSchema(schema));
            VectorizedRowBatch batch = schema.createRowBatch();
            LongColumnVector a = (LongColumnVector) batch.cols[0];      // int
            DoubleColumnVector b = (DoubleColumnVector) batch.cols[1];      // float
            DoubleColumnVector c = (DoubleColumnVector) batch.cols[2];         // double
            TimestampColumnVector d = (TimestampColumnVector) batch.cols[3];    // timestamp
            LongColumnVector e = (LongColumnVector) batch.cols[4];              // boolean
            BytesColumnVector z = (BytesColumnVector) batch.cols[5];            // string

            long curT = System.currentTimeMillis();
            Timestamp timestamp = new Timestamp(curT);
            System.out.println(curT + ", nanos: " + timestamp.getNanos() + ",  time: " + timestamp.getTime());
            for (int i = 0; i < 10; ++i)
            {
                int row = batch.size++;
                a.vector[row] = i;
                b.vector[row] = i * 3.1415f;
                c.vector[row] = i * 3.14159d;
                d.set(row, timestamp);
                e.vector[row] = i > 25000 ? 1 : 0;
                z.setVal(row, String.valueOf(i).getBytes());
                // If the batch is full, write it out and start over.
                if (batch.size == batch.getMaxSize()) {
                    try {
                        writer.addRowBatch(batch);
                    }
                    catch (IOException e2) {
                        e2.printStackTrace();
                    }
                    batch.reset();
                }
            }
            if (batch.size != 0)
            {
                try
                {
                    writer.addRowBatch(batch);
                    batch.reset();
                }
                catch (IOException e1)
                {
                    e1.printStackTrace();
                }
                batch.reset();
            }
            writer.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}
