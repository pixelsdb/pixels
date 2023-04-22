package io.pixelsdb.pixels.worker.vhive;

/**
 * @author hank
 * @date 28/06/2022
 */
public class PixelsWorkerException extends RuntimeException
{
    public PixelsWorkerException(String message)
    {
        super(message);
    }

    public PixelsWorkerException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
