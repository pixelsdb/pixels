package cn.edu.ruc.iir.orc.core.writer;

import cn.edu.ruc.iir.pixels.core.TestParams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.junit.Test;

import java.io.IOException;

import static org.apache.orc.CompressionKind.NONE;

public class TestOrcWriter {

    @Test
    public void testWriter() {
        Configuration conf = new Configuration();
        TypeDescription schema = TypeDescription.fromString("struct<id:int,x:double,y:double>");
        Writer writer = null;
        try {
            writer = OrcFile.createWriter(new Path(TestParams.orcPath),
                    OrcFile.writerOptions(conf)
                            .setSchema(schema).compress(NONE));
        } catch (IOException e) {
            e.printStackTrace();
        }
        VectorizedRowBatch batch = schema.createRowBatch();
        LongColumnVector id = (LongColumnVector) batch.cols[0];
        DoubleColumnVector x = (DoubleColumnVector) batch.cols[1];
        DoubleColumnVector y = (DoubleColumnVector) batch.cols[2];
        for (int r = 0; r < TestParams.rowNum; ++r) {
            int row = batch.size++;
            id.vector[row] = r;
            x.vector[row] = r * 1;
            y.vector[row] = r * 2;
            // If the batch is full, write it out and start over.
            if (batch.size == batch.getMaxSize()) {
                try {
                    writer.addRowBatch(batch);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                batch.reset();
            }
        }
        if (batch.size != 0) {
            try {
                writer.addRowBatch(batch);
            } catch (IOException e) {
                e.printStackTrace();
            }
            batch.reset();
        }
        try {
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
