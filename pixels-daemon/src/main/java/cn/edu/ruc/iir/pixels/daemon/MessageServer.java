package cn.edu.ruc.iir.pixels.daemon;

/**
 * pixels message server.
 * The server is responsible for communication between servers.
 *
 * @author guodong
 */
public class MessageServer implements Server
{
    @Override
    public boolean isRunning()
    {
        return Thread.currentThread().isAlive();
    }

    @Override
    public void shutDownServer()
    {

    }

    @Override
    public void run()
    {

    }
}
