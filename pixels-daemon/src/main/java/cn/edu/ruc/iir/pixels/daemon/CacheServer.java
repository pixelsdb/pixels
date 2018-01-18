package cn.edu.ruc.iir.pixels.daemon;

/**
 * pixels cache server
 *
 * @author guodong
 */
public class CacheServer implements Server
{
    @Override
    public boolean isRunning()
    {
        return Thread.currentThread().isAlive();
    }

    @Override
    public void shutDownServer()
    {
        // stop cache server
    }

    @Override
    public void run()
    {
        // start cache server
    }
}
