package io.pixelsdb.pixels.worker.vhive.utils;

public interface Server extends Runnable
{
    public boolean isRunning();

    public void shutdown();
}
