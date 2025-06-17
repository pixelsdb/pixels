package io.pixelsdb.pixels.common.exception;

public class RowIdException extends IndexException {
    public RowIdException()
    {
        super();
    }

    public RowIdException(String message)
    {
        super(message);
    }

    public RowIdException(Throwable cause)
    {
        super(cause);
    }

    public RowIdException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
