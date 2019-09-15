package io.pixelsdb.pixels.core.exception;

/**
 * pixels
 *
 * @author guodong
 */
public class PixelsRuntimeException
        extends RuntimeException
{
    private static final long serialVersionUID = -5885819000511734187L;

    public PixelsRuntimeException()
    {
        super();
    }

    public PixelsRuntimeException(String message)
    {
        super(message);
    }

    public PixelsRuntimeException(Throwable cause)
    {
        super(cause);
    }

    public PixelsRuntimeException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
