package cn.edu.ruc.iir.pixels.core.encoding;

import java.io.IOException;

/**
 * pixels
 *
 * @author guodong
 */
public abstract class Decoder
{
    public abstract boolean hasNext()
            throws IOException;
}
