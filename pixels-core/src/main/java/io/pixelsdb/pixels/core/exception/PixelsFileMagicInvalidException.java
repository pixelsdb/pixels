package io.pixelsdb.pixels.core.exception;

/**
 * pixels
 *
 * @author guodong
 */
public class PixelsFileMagicInvalidException
        extends PixelsRuntimeException
{
    private static final long serialVersionUID = 8860468509522142835L;

    public PixelsFileMagicInvalidException(String magic)
    {
        super(String.format("The file magic %s is not valid.", magic));
    }
}
