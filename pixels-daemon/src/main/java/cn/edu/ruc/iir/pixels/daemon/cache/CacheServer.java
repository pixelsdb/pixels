package cn.edu.ruc.iir.pixels.daemon.cache;

import cn.edu.ruc.iir.pixels.daemon.Server;

public class CacheServer implements Server
{
    @Override
    public boolean isRunning()
    {
        return false;
    }

    @Override
    public void shutdown()
    {

    }

    @Override
    public void run()
    {

    }
}
