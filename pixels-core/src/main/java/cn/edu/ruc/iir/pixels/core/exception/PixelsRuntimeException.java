package cn.edu.ruc.iir.pixels.core.exception;

/**
 * pixels
 *
 * @author guodong
 */
public class PixelsRuntimeException extends RuntimeException
{
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
