package cn.edu.ruc.iir.orc.core.reader;

import cn.edu.ruc.iir.pixels.core.TestParams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.junit.Test;

import java.io.IOException;

public class TestOrcReader {

    @Test
    public void testReader() throws IOException {
        Configuration conf = new Configuration();
        Reader reader = null;
        try {
            reader = OrcFile.createReader(new Path(TestParams.orcPath),
                    OrcFile.readerOptions(conf));
        } catch (IOException e) {
            e.printStackTrace();
        }
        RecordReader rows = null;
        try {
            rows = reader.rows();
        } catch (IOException e) {
            e.printStackTrace();
        }
        VectorizedRowBatch batch = reader.getSchema().createRowBatch();
        try {
            int i = 0;
            while (rows.nextBatch(batch)) {
                System.out.println(i++);
                System.out.println("Row Number: " + rows.getRowNumber());
                System.out.println("Batch Size: " + batch.size);
                for (int r = 0; r < batch.size; ++r) {
                    System.out.println(batch.toString());
                    break;
                }
                break;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            rows.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
