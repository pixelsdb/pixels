package cn.edu.ruc.iir.pixels.core.encoding;

import cn.edu.ruc.iir.pixels.core.TestParams;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static org.junit.Assert.assertArrayEquals;

/**
 * pixels
 *
 * @author guodong
 */
public class TestEncoding
{
    @Test
    public void runLengthTest()
    {
        long[] values = new long[TestParams.rowNum];
        for (int i = 0; i < TestParams.rowNum; i++)
        {
            values[i] = i;
        }
        long[] decoderValues = new long[TestParams.rowNum];
        RleEncoder encoder = new RleEncoder(false, true);
        try
        {
            byte[] bytes = encoder.encode(values);
            IntDecoder decoder = new RleDecoder(new ByteArrayInputStream(bytes), false);
            int i = 0;
            while (decoder.hasNext()) {
                decoderValues[i++] = decoder.next();
            }
            System.out.println(bytes.length);
            assertArrayEquals(values, decoderValues);
        } catch (IOException e)
        {
            e.printStackTrace();
        }
    }
}
