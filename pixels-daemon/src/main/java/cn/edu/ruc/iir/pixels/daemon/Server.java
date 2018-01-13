package cn.edu.ruc.iir.pixels.daemon;

public interface Server extends Runnable
{
    public boolean isRunning();

    public void shutDownServer ();
}
