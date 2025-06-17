package io.pixelsdb.pixels.common.exception;

public class SecondaryIndexException extends IndexException
{
    public SecondaryIndexException()
    {
        super();
    }

    public SecondaryIndexException(String message)
    {
        super(message);
    }

    public SecondaryIndexException(Throwable cause)
    {
        super(cause);
    }

    public SecondaryIndexException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
