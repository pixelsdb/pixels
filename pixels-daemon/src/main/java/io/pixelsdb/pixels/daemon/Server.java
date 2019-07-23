package io.pixelsdb.pixels.daemon;

public interface Server extends Runnable
{
    public boolean isRunning();

    public void shutdown();
}
