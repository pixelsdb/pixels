package cn.edu.ruc.iir.pixels.daemon;

import cn.edu.ruc.iir.pixels.common.utils.ConfigFactory;
import cn.edu.ruc.iir.pixels.daemon.cache.CacheCoordinator;
import cn.edu.ruc.iir.pixels.daemon.cache.CacheManager;
import cn.edu.ruc.iir.pixels.daemon.exception.NoSuchServerException;
import cn.edu.ruc.iir.pixels.daemon.metadata.MetadataServer;
import cn.edu.ruc.iir.pixels.daemon.metric.MetricsServer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.TimeUnit;

/**
 * example command to start pixels-daemon:
 * java -Dio.netty.leakDetection.level=advanced -Drole=main -jar pixels-daemon-0.1.0-SNAPSHOT-full.jar datanode|coordinator
 * */
public class DaemonMain
{
    private static Logger log = LogManager.getLogger(DaemonMain.class);

    public static void main(String[] args)
    {
        String role = System.getProperty("role");

        if (role != null)
        {
            String mainFile = ConfigFactory.Instance().getProperty("file.lock.main");
            String guardFile = ConfigFactory.Instance().getProperty("file.lock.guard");
            String jarName = ConfigFactory.Instance().getProperty("daemon.jar");
            String daemonJarPath = ConfigFactory.Instance().getProperty("pixels.home") + jarName;

            if (role.equalsIgnoreCase("main") && args.length == 1 &&
                    (args[0].equalsIgnoreCase("coordinator") || args[0].equalsIgnoreCase("datanode")))
            {
                // this is the main daemon.
                System.out.println("starting main daemon...");
                Daemon mainDaemon = new Daemon();
                String[] guardCmd = {"java", "-Drole=guard", "-jar", daemonJarPath, args[0]};
                mainDaemon.setup(mainFile, guardFile, guardCmd);
                // the main daemon logic will be running in a thread,
                // and it will start the gard daemon process to protect each other.
                Thread daemonThread = new Thread(mainDaemon);
                daemonThread.setName("main daemon thread");
                // jvm will not wait for a daemon thread to terminate.
                daemonThread.setDaemon(true);
                daemonThread.start();

                try
                {
                    // wait for the daemon thread to start.
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e)
                {
                    log.error("error when waiting for the main daemon thread to start.", e);
                }

                ServerContainer container = new ServerContainer();

                if (args[0].equalsIgnoreCase("coordinator"))
                {
                    ConfigFactory config = ConfigFactory.Instance();
                    int port = Integer.valueOf(config.getProperty("metadata.server.port"));

                    // start metadata
                    MetadataServer metadataServer = new MetadataServer(port);
                    container.addServer("metadata", metadataServer);
                    // start cache coordinator
                    CacheCoordinator cacheCoordinator = new CacheCoordinator();
                    container.addServer("cache_coordinator", cacheCoordinator);
                }
                else
                {
                    // start metrics server and cache manager on data node
                    MetricsServer metricsServer = new MetricsServer();
                    container.addServer("metrics", metricsServer);
                    CacheManager cacheManager = new CacheManager();
                    container.addServer("cache_manager", cacheManager);
                }

                // continue the main thread, start and check the server threads.
                // main thread will be terminated with the daemon thread.
                while (mainDaemon.isRunning())
                {
                    try
                    {
                        for (String name : container.getServerNames())
                        {
                            if (container.chechServer(name) == false)
                            {
                                container.startServer(name);
                            }
                        }
                        TimeUnit.SECONDS.sleep(3);
                    } catch (Exception e)
                    {
                        log.error("error in the main loop of daemon.", e);
                        break;
                    }
                }

                for (String name : container.getServerNames())
                {
                    // shutdown the server threads.
                    try
                    {
                        container.shutdownServer(name);
                    } catch (NoSuchServerException e)
                    {
                        log.error("error when stoping server threads.", e);
                    }
                }
            }
            else if (role.equalsIgnoreCase("guard") && args.length == 1 &&
                    (args[0].equalsIgnoreCase("coordinator") ||
                            args[0].equalsIgnoreCase("datanode")))
            {
                // this is the guard daemon
                System.out.println("starting guard daemon...");
                Daemon guardDaemon = new Daemon();
                String[] mainCmd = {"java", "-Drole=main", "-jar", daemonJarPath, args[0]};
                // start the guard daemon, and guard daemon and the main daemon protect each other.
                guardDaemon.setup(guardFile, mainFile, mainCmd);
                // run() contains an endless loop().
                guardDaemon.run();
            }
            else if (role.equalsIgnoreCase("kill"))
            {
                System.out.println("Shutdown Daemons...");
                try
                {
                    Process process = Runtime.getRuntime().exec("jps -lv");
                    BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
                    String line;
                    while ((line = reader.readLine()) != null)
                    {
                        String[] splits = line.split("\\s{1,}");
                        if (splits.length < 3)
                        {
                            continue;
                        }
                        if (splits[1].contains(jarName) || splits[1].contains(DaemonMain.class.getName()))
                        {
                            String roleName = null;
                            // get the role name of the target daemon (to be killing).
                            for (int i = 2; i < splits.length; ++i)
                            {
                                if ((splits[i].contains("-Drole=main") || splits[i].contains("-Drole=guard")))
                                {
                                    roleName = splits[i].split("=")[1];
                                    break;
                                }
                            }
                            if (roleName == null)
                            {
                                continue;
                            }
                            int pid = Integer.parseInt(splits[0]);
                            System.out.println("killing " + roleName + ", pid (" + pid + ")");
                            // terminate the daemon gracefully by sending SIGTERM(15) signal.
                            Runtime.getRuntime().exec("kill -9 " + pid);
                        }
                    }
                    reader.close();
                    process.destroy();
                } catch (IOException e)
                {
                    log.error("error when killing pixels daemons.", e);
                }
            }
            else
            {
                System.err.println("Run with -Drole={main,guard,kill} {coordinator/datanode}");
            }
        }
        else
        {
            System.err.println("Run with -Drole={main,guard,kill} {coordinator/datanode}");
        }
    }
}
