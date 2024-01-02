package io.pixelsdb.pixels.daemon;

import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.daemon.cache.CacheCoordinator;
import io.pixelsdb.pixels.daemon.cache.CacheManager;
import io.pixelsdb.pixels.daemon.exception.NoSuchServerException;
import io.pixelsdb.pixels.daemon.metadata.MetadataServer;
import io.pixelsdb.pixels.daemon.metrics.MetricsServer;
import io.pixelsdb.pixels.daemon.transaction.TransServer;
import io.pixelsdb.pixels.daemon.turbo.QueryScheduleServer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.TimeUnit;

/**
 * example command to start pixels-daemon:
 * java -Dio.netty.leakDetection.level=advanced -Drole=main -jar pixels-daemon-0.2.0-SNAPSHOT-full.jar datanode|coordinator
 * */
public class DaemonMain
{
    private static final Logger log = LogManager.getLogger(DaemonMain.class);
    private static final String GuardScript = "bin/start-guard.sh";
    private static final String CoordinatorScript = "bin/start-coordinator.sh";
    private static final String DataNodeScript = "bin/start-datanode.sh";

    public static void main(String[] args)
    {
        String role = System.getProperty("role");

        if (role != null)
        {
            String varDir = ConfigFactory.Instance().getProperty("pixels.var.dir");
            String pixelsHome = ConfigFactory.Instance().getProperty("pixels.home");
            String mainFile = null;
            String guardFile = null;

            if (args.length == 1)
            {
                if (!varDir.endsWith("/") && !varDir.endsWith("\\"))
                {
                    varDir += "/";
                }
                mainFile = varDir + "pixels." + args[0].toLowerCase() + ".main.lock";
                guardFile = varDir + "pixels." + args[0].toLowerCase() + ".guard.lock";
            }

            if (role.equalsIgnoreCase("main") && args.length == 1 &&
                    (args[0].equalsIgnoreCase("coordinator") ||
                            args[0].equalsIgnoreCase("datanode")))
            {
                // this is the main daemon.
                System.out.println("starting main daemon...");
                Daemon mainDaemon = new Daemon();
                String[] guardCmd = {pixelsHome + GuardScript, "-daemon", args[0]};
                mainDaemon.setup(mainFile, guardFile, guardCmd);
                // the main daemon logic will be running in a thread,
                // and it will start the gard daemon process to protect each other.
                Thread daemonThread = new Thread(mainDaemon);
                daemonThread.setName("main daemon thread");
                daemonThread.start();

                try
                {
                    // wait for the daemon thread to start.
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e)
                {
                    log.error("error when waiting for the main daemon thread to start", e);
                }

                ServerContainer container = new ServerContainer();
                ConfigFactory config = ConfigFactory.Instance();
                boolean cacheEnabled = Boolean.parseBoolean(config.getProperty("cache.enabled"));

                if (args[0].equalsIgnoreCase("coordinator"))
                {
                    int metadataServerPort = Integer.parseInt(config.getProperty("metadata.server.port"));
                    int transServerPort = Integer.parseInt(config.getProperty("trans.server.port"));
                    int queryScheduleServerPort = Integer.parseInt(config.getProperty("query.schedule.server.port"));

                    try
                    {
                        // start metadata server
                        MetadataServer metadataServer = new MetadataServer(metadataServerPort);
                        container.addServer("metadata", metadataServer);
                        // start transaction server
                        TransServer transServer = new TransServer(transServerPort);
                        container.addServer("transaction", transServer);
                        // start query schedule server
                        QueryScheduleServer queryScheduleServer = new QueryScheduleServer(queryScheduleServerPort);
                        container.addServer("query_schedule", queryScheduleServer);
                        if (cacheEnabled)
                        {
                            // start cache coordinator
                            CacheCoordinator cacheCoordinator = new CacheCoordinator();
                            container.addServer("cache_coordinator", cacheCoordinator);
                        }
                    } catch (Throwable e)
                    {
                        log.error("failed to start coordinator", e);
                    }
                }
                else
                {
                    boolean metricsServerEnabled = Boolean.parseBoolean(
                            ConfigFactory.Instance().getProperty("metrics.server.enabled"));
                    try
                    {
                        // start metrics server and cache manager on data node
                        if (metricsServerEnabled)
                        {
                            MetricsServer metricsServer = new MetricsServer();
                            container.addServer("metrics", metricsServer);
                        }
                        if (cacheEnabled)
                        {
                            CacheManager cacheManager = new CacheManager();
                            container.addServer("cache_manager", cacheManager);
                        }
                    } catch (Throwable e)
                    {
                        log.error("failed to start node manager", e);
                    }
                }

                // The shutdown hook ensures the servers are shutdown graceful
                // if this main daemon is terminated by SIGTERM(15) signal.
                Runtime.getRuntime().addShutdownHook( new Thread( () ->
                {
                    for (String name : container.getServerNames())
                    {
                        // shutdown the server threads.
                        try
                        {
                            container.shutdownServer(name);
                        } catch (NoSuchServerException e)
                        {
                            log.error("error when stopping server threads", e);
                        }
                    }
                    for (int i = 60; i > 0; --i)
                    {
                        // System.out.print("\rRemaining (" + i + ")s for server threads to shutdown...");
                        try
                        {
                            boolean done = true;
                            for (String name : container.getServerNames())
                            {
                                if (container.checkServer(name, 0))
                                {
                                    done = false;
                                    break;
                                }
                            }
                            if (done)
                            {
                                break;
                            }
                            TimeUnit.SECONDS.sleep(1);
                        } catch (Throwable e)
                        {
                            log.error("error when waiting server threads shutdown", e);
                        }
                    }
                    /**
                     * Issue #181:
                     * Shutdown the daemon thread here instead of using the SIGTERM handler.
                     */
                    mainDaemon.shutdown();
                    log.info("All the servers and the daemon thread are shutdown, byte...");
                }));

                // continue the main thread, start and check the server threads.
                // main thread will be terminated with the daemon thread.
                while (mainDaemon.isRunning())
                {
                    try
                    {
                        for (String name : container.getServerNames())
                        {
                            if (!container.checkServer(name))
                            {
                                container.startServer(name);
                            }
                        }
                        TimeUnit.SECONDS.sleep(1);
                    } catch (Throwable e)
                    {
                        log.error("error in the main loop of daemon", e);
                        break;
                    }
                }
                // Will never reach here.
            }
            else if (role.equalsIgnoreCase("guard") && args.length == 1 &&
                    (args[0].equalsIgnoreCase("coordinator") ||
                            args[0].equalsIgnoreCase("datanode")))
            {
                // this is the guard daemon
                System.out.println("starting guard daemon...");
                Daemon guardDaemon = new Daemon();
                String script;
                if (args[0].equalsIgnoreCase("coordinator"))
                {
                    script = CoordinatorScript;
                } else
                {
                    script = DataNodeScript;
                }
                String[] mainCmd = {pixelsHome + script, "-daemon"};
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
                        if (splits[1].contains(PixelsCoordinator.class.getName()) ||
                                splits[1].contains(PixelsDataNode.class.getName()) ||
                                splits[1].contains(PixelsGuard.class.getName()))
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
                            // Terminate the daemon gracefully by sending SIGTERM(15) signal.
                            Runtime.getRuntime().exec("kill -15 " + pid);
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
