package cn.edu.ruc.iir.pixels.daemon;

import cn.edu.ruc.iir.pixels.common.ConfigFactory;

import java.util.concurrent.TimeUnit;

public class DaemonMain
{
    public static void main(String[] args)
    {
        boolean isMain = false;
        if (args.length > 0)
        {
            if (args[0].equalsIgnoreCase("main"))
            {
                isMain = true;
            }
        }

        String mainFile = ConfigFactory.Instance().getProperty("file.lock.main");
        String guardFile = ConfigFactory.Instance().getProperty("file.lock.guard");

        if (isMain)
        {
            // this is the main daemon
            GuardDaemon guardDaemon = new GuardDaemon();
            String[] guardCmd = {"java", "-jar", "pixels-daemon-0.1.0-SNAPSHOT-full.jar", "guard"};
            guardDaemon.setup(mainFile, guardFile, guardCmd);
            Thread daemonThread = new Thread(guardDaemon);
            daemonThread.setName("main daemon thread");
            daemonThread.setDaemon(true);
            daemonThread.start();

            // continue the main thread
            while (true)
            {
                try
                {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e)
                {
                    e.printStackTrace();
                }
            }
        }
        else
        {
            // this is the guard daemon
            GuardDaemon guardDaemon = new GuardDaemon();
            String[] guardCmd = {"java", "-jar", "pixels-daemon-0.1.0-SNAPSHOT-full.jar", "main"};
            guardDaemon.setup(guardFile, mainFile, guardCmd);
            guardDaemon.run();
        }
    }
}
