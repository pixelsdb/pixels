package io.pixelsdb.pixels.worker.vhive;

public interface Server extends Runnable
{
    public boolean isRunning();

    public void shutdown();
}
