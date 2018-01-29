package cn.edu.ruc.iir.pixels.daemon;

import cn.edu.ruc.iir.pixels.common.ConfigFactory;
import cn.edu.ruc.iir.pixels.common.LogFactory;
import cn.edu.ruc.iir.pixels.metadata.server.MetadataServer;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class DaemonMain
{
    public static void main(String[] args)
    {
        if (args.length > 0)
        {
            String mainFile = ConfigFactory.Instance().getProperty("file.lock.main");
            String guardFile = ConfigFactory.Instance().getProperty("file.lock.guard");

            if (args[0].equalsIgnoreCase("main"))
            {
                // this is the main daemon
                System.out.println("starting main daemon...");
                Daemon guardDaemon = new Daemon();
                String[] guardCmd = {"java", "-jar", "pixels-daemon-0.1.0-SNAPSHOT-full.jar", "guard"};
                guardDaemon.setup(mainFile, guardFile, guardCmd);
                Thread daemonThread = new Thread(guardDaemon);
                daemonThread.setName("main daemon thread");
                daemonThread.setDaemon(true);
                daemonThread.start();

                // continue the main thread
                while (true)
                {
                    Server server = new TomcatServer();
                    try
                    {
                        if (!server.isRunning())
                        {
                            boolean serverIsDown = true;

                            for (int i = 0; i < 3; ++i)
                            {
                                // try 3 times
                                TimeUnit.SECONDS.sleep(1);
                                if (server.isRunning())
                                {
                                    serverIsDown = false;
                                }
                            }

                            if (serverIsDown)
                            {
                                server.shutDownServer();
                                Thread serverThread = new Thread(server);
                                serverThread.start();
                                TimeUnit.SECONDS.sleep(5);
                            }
                        }
                        TimeUnit.SECONDS.sleep(3);

                    } catch (Exception e)
                    {
                        e.printStackTrace();
                    }
                }
            } else if (args[0].equalsIgnoreCase("guard"))
            {
                // this is the guard daemon
                System.out.println("starting guard daemon...");
                Daemon guardDaemon = new Daemon();
                String[] guardCmd = {"java", "-jar", "pixels-daemon-0.1.0-SNAPSHOT-full.jar", "main"};
                guardDaemon.setup(guardFile, mainFile, guardCmd);
                guardDaemon.run();
            } else if (args[0].equalsIgnoreCase("shutdown"))
            {
                System.out.println("Shutdown Daemons..");
                try
                {
                    for (int i = 1; i < args.length; ++i)
                    {
                        int pid = Integer.parseInt(args[i]);
                        System.out.println(pid);
                        Runtime.getRuntime().exec("kill -9 " + pid);
                    }
                } catch (IOException e)
                {
                    LogFactory.Instance().getLog().error("error when killing pixels daemons.", e);
                }
            }
        }
        else
        {
            ConfigFactory config = ConfigFactory.Instance();
            int port = Integer.valueOf(config.getProperty("port"));
            System.out.println("Metadata Server binding port " + port);
            MetadataServer server = new MetadataServer();
            try {
                server.bind(port);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
