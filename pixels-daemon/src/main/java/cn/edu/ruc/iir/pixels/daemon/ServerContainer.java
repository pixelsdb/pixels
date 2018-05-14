package cn.edu.ruc.iir.pixels.daemon;

import cn.edu.ruc.iir.pixels.common.utils.LogFactory;
import cn.edu.ruc.iir.pixels.daemon.exception.NoSuchServerException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class ServerContainer
{
    private Map<String, Server> serverMap = null;

    public ServerContainer ()
    {
        this.serverMap = new HashMap<>();
    }

    public void addServer (String name, Server server)
    {
        Thread thread = new Thread(server);
        thread.start();
        this.serverMap.put(name, server);
    }

    public List<String> getServerNames ()
    {
        return new ArrayList<>(this.serverMap.keySet());
    }

    public boolean chechServer (String name) throws NoSuchServerException
    {
        Server server = this.serverMap.get(name);
        if (server == null)
        {
            throw new NoSuchServerException();
        }
        boolean serverIsRunning = false;
        try
        {
            if (!server.isRunning())
            {
                for (int i = 0; i < 3; ++i)
                {
                    // try 3 times
                    TimeUnit.SECONDS.sleep(1);
                    if (server.isRunning())
                    {
                        serverIsRunning = true;
                        break;
                    }
                }
            }
            else
            {
                serverIsRunning = true;
            }
        } catch (InterruptedException e)
        {
            LogFactory.Instance().getLog().error(
                    "interrupted while checking server.", e);
        }
        return serverIsRunning;
    }

    public void startServer (String name) throws NoSuchServerException
    {
        Server server = this.serverMap.get(name);
        if (server == null)
        {
            throw new NoSuchServerException();
        }
        if (chechServer(name) == true)
        {
            server.shutdown();
        }
        Thread serverThread = new Thread(server);
        serverThread.start();
    }
}
