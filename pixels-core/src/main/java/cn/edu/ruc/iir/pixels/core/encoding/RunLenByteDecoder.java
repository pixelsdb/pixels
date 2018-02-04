package cn.edu.ruc.iir.pixels.core.encoding;

import java.io.IOException;

/**
 * pixels
 *
 * @author guodong
 */
public class RunLenByteDecoder
        extends Decoder
{
    @Override
    public boolean hasNext() throws IOException
    {
        return false;
    }

    public byte next()
    {
        return (byte) 1;
    }
}
