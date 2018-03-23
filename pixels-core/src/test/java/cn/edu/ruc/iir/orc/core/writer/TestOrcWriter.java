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
import java.util.Random;

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

    @Test
    public void testWriterAll() {
        Configuration conf = new Configuration();
        TypeDescription schema = TypeDescription.fromString("struct<a:int,b:float,c:double,d:timestamp,e:boolean,z:string>");
        Writer writer = null;
        try {
            writer = OrcFile.createWriter(new Path(TestParams.orcPath_6),
                    OrcFile.writerOptions(conf)
                            .setSchema(schema).compress(NONE));
        } catch (IOException e) {
            e.printStackTrace();
        }
        VectorizedRowBatch batch = schema.createRowBatch();
        LongColumnVector a = (LongColumnVector) batch.cols[0];
        DoubleColumnVector b = (DoubleColumnVector) batch.cols[1];
        DoubleColumnVector c = (DoubleColumnVector) batch.cols[2];
        TimestampColumnVector d = (TimestampColumnVector) batch.cols[3];
        LongColumnVector e = (LongColumnVector) batch.cols[4];
        BytesColumnVector z = (BytesColumnVector) batch.cols[5];
        Random randomKey = new Random();
        Random randomSf = new Random(System.currentTimeMillis() * randomKey.nextInt());

        long curT = System.currentTimeMillis();
        Timestamp timestamp = new Timestamp(curT);

        for (int r = 0; r < TestParams.rowNum; ++r) {
            int key = randomKey.nextInt(50000);
            float sf = randomSf.nextFloat();
            double sd = randomSf.nextDouble();
            int row = batch.size++;
            a.vector[row] = r;
            b.vector[row] = r * 3.1415f;
            c.vector[row] = r * 3.14159d;
            d.set(row, timestamp);
            e.vector[row] = r > 25000 ? 1 : 0;
            z.setVal(row, String.valueOf(r).getBytes());
            // If the batch is full, write it out and start over.
            if (batch.size == batch.getMaxSize()) {
                try {
                    writer.addRowBatch(batch);
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
                batch.reset();
            }
        }
        if (batch.size != 0) {
            try {
                writer.addRowBatch(batch);
            } catch (IOException e1) {
                e1.printStackTrace();
            }
            batch.reset();
        }
        try {
            writer.close();
        } catch (IOException e1) {
            e1.printStackTrace();
        }
    }

}
