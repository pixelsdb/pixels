package io.pixelsdb.pixels.storage.s3qs.exception;

public class TaskErrorException extends Exception
{
    public TaskErrorException(String msg)
        {
            super(msg);
        }
}
