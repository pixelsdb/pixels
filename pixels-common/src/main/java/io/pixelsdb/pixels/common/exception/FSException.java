package io.pixelsdb.pixels.common.exception;

/**
 * Created at: 18-10-17
 * Author: hank
 */
public class FSException extends Exception
{
    public FSException()
    {
        super();
    }

    public FSException(String message)
    {
        super(message);
    }

    public FSException(Throwable cause)
    {
        super(cause);
    }

    public FSException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
