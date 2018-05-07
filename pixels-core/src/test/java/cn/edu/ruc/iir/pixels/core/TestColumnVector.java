package cn.edu.ruc.iir.pixels.core;

import cn.edu.ruc.iir.pixels.core.vector.ColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.DoubleColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.LongColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.TimestampColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.VectorizedRowBatch;
import org.junit.Test;

import java.sql.Timestamp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * pixels
 *
 * @author guodong
 */
public class TestColumnVector
{
    @Test
    public void testCVSet()
    {
        LongColumnVector a = new LongColumnVector(100);
        for (int i = 0; i < 100; i++) {
            a.vector[i] = i;
        }
        ColumnVector b = new LongColumnVector(100);
        for (int i = 0; i < 100; i++)
        {
            b.setElement(i, i, a);
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < b.getLength(); i++) {
            b.stringifyValue(sb, i);
            sb.append("\n");
        }
        System.out.println(sb.toString());
    }

    @Test
    public void testCVCopyFrom()
    {
        int testNum = 1000_000;
        String mockSchema = "struct<a:int,b:double,c:string,d:timestamp>";
        
        VectorizedRowBatch srcRowBatch = TypeDescription.fromString(mockSchema).createRowBatch(testNum);
        LongColumnVector src0 = (LongColumnVector) srcRowBatch.cols[0];
        DoubleColumnVector src1 = (DoubleColumnVector) srcRowBatch.cols[1];
//        BytesColumnVector src2 = (BytesColumnVector) srcRowBatch.cols[2];
        TimestampColumnVector src3 = (TimestampColumnVector) srcRowBatch.cols[3];

        VectorizedRowBatch dstRowBatch = TypeDescription.fromString(mockSchema).createRowBatch(testNum);
        LongColumnVector dst0 = (LongColumnVector) dstRowBatch.cols[0];
        DoubleColumnVector dst1 = (DoubleColumnVector) dstRowBatch.cols[1];
//        BytesColumnVector dst2 = (BytesColumnVector) dstRowBatch.cols[2];
        TimestampColumnVector dst3 = (TimestampColumnVector) dstRowBatch.cols[3];

        for (int i = 0; i < testNum; i++) {
            src0.vector[i] = i;
            src1.vector[i] = i * 1.0d;
//            src2.setVal(i, String.valueOf(i).getBytes());
            src3.set(i, Timestamp.valueOf("2018-05-07 20:39:20"));
        }

        long begin = System.nanoTime();
        dst0.copyFrom(src0);
        dst1.copyFrom(src1);
//        dst2.copyFrom(src2);
        dst3.copyFrom(src3);
        long end = System.nanoTime();
        System.out.println("Copy cost: " + (end - begin));

        for (int i = 0; i < testNum; i++) {
            assert dst0.vector[i] == i;
            assert i * 1.0d == dst1.vector[i];
//            assertEquals(String.valueOf(i), dst2.toString(i));
            assertEquals(Timestamp.valueOf("2018-05-07 20:39:20"), dst3.asScratchTimestamp(i));
        }
    }

    @Test
    public void testColumnDuplication()
    {
        String mockSchema = "struct<a:int,b:string,c:double,d:int,a:int,b:string,e:boolean>";
        VectorizedRowBatch rowBatch = TypeDescription.fromString(mockSchema).createRowBatch();

        assertFalse(rowBatch.cols[0].duplicated);
        assert rowBatch.cols[0].originVecId == -1;
        assertFalse(rowBatch.cols[1].duplicated);
        assert rowBatch.cols[1].originVecId == -1;
        assertFalse(rowBatch.cols[2].duplicated);
        assert rowBatch.cols[2].originVecId == -1;
        assertFalse(rowBatch.cols[3].duplicated);
        assert rowBatch.cols[3].originVecId == -1;
        assertTrue(rowBatch.cols[4].duplicated);
        assert rowBatch.cols[4].originVecId == 0;
        assertTrue(rowBatch.cols[5].duplicated);
        assert rowBatch.cols[5].originVecId == 1;
        assertFalse(rowBatch.cols[6].duplicated);
        assert rowBatch.cols[6].originVecId == -1;
    }
}
