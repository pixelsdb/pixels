package io.pixelsdb.pixels.storage.s3qs.exception;

/**
 * @author yanhaoting
 * @create 2025-12-19
 */
public class TaskErrorException extends Exception
{
    public TaskErrorException(String msg)
        {
            super(msg);
        }
}
