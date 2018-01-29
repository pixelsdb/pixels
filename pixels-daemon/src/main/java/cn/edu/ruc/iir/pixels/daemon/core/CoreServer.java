package cn.edu.ruc.iir.pixels.daemon.core;

import cn.edu.ruc.iir.pixels.daemon.Server;

public class CoreServer implements Server
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
