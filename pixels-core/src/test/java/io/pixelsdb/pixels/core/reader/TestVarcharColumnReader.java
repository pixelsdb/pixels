package io.pixelsdb.pixels.core.reader;

import com.google.errorprone.annotations.Var;
import io.pixelsdb.pixels.core.PixelsProto;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.encoding.EncodingLevel;
import io.pixelsdb.pixels.core.vector.BinaryColumnVector;
import io.pixelsdb.pixels.core.writer.PixelsWriterOption;
import io.pixelsdb.pixels.core.writer.VarcharColumnWriter;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static io.pixelsdb.pixels.core.reader.ColumnReader.newColumnReader;
import static io.pixelsdb.pixels.core.writer.ColumnWriter.newColumnWriter;

public class TestVarcharColumnReader
{
    @Test
    public void test() throws IOException
    {
        int batchNum = 300;
        int rowNum = 1024;
        int varCharMaxSize = 199;
        PixelsWriterOption writerOption = new PixelsWriterOption()
                .pixelStride(10000).byteOrder(ByteOrder.LITTLE_ENDIAN)
                .encodingLevel(EncodingLevel.EL0).nullsPadding(false);
        VarcharColumnWriter columnWriter = (VarcharColumnWriter) newColumnWriter(TypeDescription.createVarchar(varCharMaxSize), writerOption);

        // construct source vector
        BinaryColumnVector columnVector = new BinaryColumnVector(rowNum);
        for (int j = 0; j < rowNum; j++)
        {
            if (j % 100 == 0)
            {
                columnVector.addNull();
            } else
            {
                int len = j%varCharMaxSize + 1;
                char[] charArray = new char[len];
                for (int k = 0; k < len; k++)
                {
                    charArray[k] = (char) k;
                }
                String str = new String(charArray);
                columnVector.add(str);
            }
        }
        for (int i = 0; i < batchNum; i++)
        {
            columnWriter.write(columnVector, rowNum);
        }
        columnWriter.flush();
        byte[] content = columnWriter.getColumnChunkContent();
        PixelsProto.ColumnChunkIndex chunkIndex = columnWriter.getColumnChunkIndex().build();
        PixelsProto.ColumnEncoding encoding = columnWriter.getColumnChunkEncoding().build();
        VarcharColumnReader columnReader = (VarcharColumnReader) newColumnReader(TypeDescription.createVarchar(varCharMaxSize), null);
        BinaryColumnVector binaryColumnVector = new BinaryColumnVector(batchNum*rowNum);
        columnReader.read(ByteBuffer.wrap(content), encoding, 0, batchNum*rowNum, 10000,
                0, binaryColumnVector, chunkIndex);
        for (int i = 0; i < batchNum; i++)
        {
            for (int j = 0; j < rowNum; j++)
            {
                if (j % 100 == 0)
                {
                    assert columnVector.isNull[j] == binaryColumnVector.isNull[j];
                } else
                {
                    String s1 = new String(columnVector.vector[j], columnVector.start[j], columnVector.lens[j]);
                    String s2 = new String(binaryColumnVector.vector[i*rowNum+j],
                            binaryColumnVector.start[i*rowNum+j], binaryColumnVector.lens[i*rowNum+j]);
                    if (!s1.equals(s2))
                    {
                        assert false;
                    }
                }
            }
        }
    }
}
