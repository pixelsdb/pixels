package io.pixelsdb.pixels.core.encoding;

import java.io.IOException;

/**
 * pixels
 *
 * @author guodong
 */
public abstract class IntDecoder extends Decoder
{
    public abstract long next()
            throws IOException;
}
