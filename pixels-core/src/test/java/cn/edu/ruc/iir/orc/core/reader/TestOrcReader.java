package cn.edu.ruc.iir.orc.core.reader;

import cn.edu.ruc.iir.pixels.core.TestParams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.junit.Test;

import java.io.IOException;

public class TestOrcReader {

    @Test
    public void testReader()
    {
        Configuration conf = new Configuration();
        Reader reader = null;
        try {
            reader = OrcFile.createReader(new Path(TestParams.orcPath),
                    OrcFile.readerOptions(conf));
            RecordReader rows = null;
            rows = reader.rows();
            VectorizedRowBatch batch = reader.getSchema().createRowBatch();
            long num = 0;
            long begin = System.currentTimeMillis();
            while (rows.nextBatch(batch)) {
                num += batch.size;
            }
            long end = System.currentTimeMillis();
            System.out.println("Size: " + num);
            System.out.println("Time: " + (end - begin));
            rows.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testPixelWriterByOrcReader()
    {
        Configuration conf = new Configuration();
        Reader reader = null;
        try {
            reader = OrcFile.createReader(new Path(TestParams.orcPath),
                    OrcFile.readerOptions(conf));
            RecordReader rows = null;
            rows = reader.rows();
            VectorizedRowBatch batch = reader.getSchema().createRowBatch();
            long num = 0;
            long begin = System.currentTimeMillis();
            while (rows.nextBatch(batch)) {
                num += batch.size;
//                System.out.println(batch.toString());
                int i, j, k;
                StringBuilder b = new StringBuilder();
                for(i = 0; i < batch.size; ++i) {
                    for(k = 0; k < batch.projectionSize; ++k) {
                        int projIndex = batch.projectedColumns[k];
                        ColumnVector cv = batch.cols[projIndex];
                        cv.stringifyValue(b, i);
                    }
                    System.out.println(b.toString());
                    if(i == 1)
                        break;
                }
                break;
            }
            long end = System.currentTimeMillis();
            System.out.println("Size: " + num);
            System.out.println("Time: " + (end - begin));
            rows.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
