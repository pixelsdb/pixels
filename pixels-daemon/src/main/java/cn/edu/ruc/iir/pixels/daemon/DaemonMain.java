package cn.edu.ruc.iir.pixels.daemon;

import cn.edu.ruc.iir.pixels.common.ConfigFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
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
            } else if (args[0].equalsIgnoreCase("shutdown"))
            {
                System.out.println("Shutdown Daemons..");
                try
                {
                    for (int i = 0; i < 3; ++i)
                    {
                        Process process = Runtime.getRuntime().exec("jps -l");
                        BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
                        String line;
                        while ((line = reader.readLine()) != null)
                        {
                            System.out.println(line);
                            if (line.toLowerCase().contains("pixels-daemon"))
                            {
                                String[] strings = line.split("\\s{1,}");
                                int pid = Integer.parseInt(strings[0]);
                                System.out.println(pid);
                                Runtime.getRuntime().exec("kill -9 " + pid);
                            }
                        }
                        process.waitFor();
                        reader.close();
                        process.destroy();
                    }
                } catch (IOException e)
                {
                    e.printStackTrace();
                } catch (InterruptedException e)
                {
                    e.printStackTrace();
                }
                System.exit(0);
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
        } else
        {
            // this is the guard daemon
            GuardDaemon guardDaemon = new GuardDaemon();
            String[] guardCmd = {"java", "-jar", "pixels-daemon-0.1.0-SNAPSHOT-full.jar", "main"};
            guardDaemon.setup(guardFile, mainFile, guardCmd);
            guardDaemon.run();
        }
    }
}
