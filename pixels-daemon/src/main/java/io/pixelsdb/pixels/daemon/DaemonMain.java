package io.pixelsdb.pixels.daemon;

import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.daemon.cache.CacheCoordinator;
import io.pixelsdb.pixels.daemon.cache.CacheWorker;
import io.pixelsdb.pixels.daemon.retina.RetinaServer;
import io.pixelsdb.pixels.daemon.exception.NoSuchServerException;
import io.pixelsdb.pixels.daemon.heartbeat.HeartbeatCoordinator;
import io.pixelsdb.pixels.daemon.heartbeat.HeartbeatWorker;
import io.pixelsdb.pixels.daemon.metadata.MetadataServer;
import io.pixelsdb.pixels.daemon.metrics.MetricsServer;
import io.pixelsdb.pixels.daemon.scaling.ScalingMetricsServer;
import io.pixelsdb.pixels.daemon.sink.SinkServer;
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
 * java -Dio.netty.leakDetection.level=advanced -Doperation=start|stop -Drole=coordinator|worker -jar pixels-daemon-0.2.0-SNAPSHOT-full.jar
 * */
public class DaemonMain
{
    private static final Logger log = LogManager.getLogger(DaemonMain.class);

    public static void main(String[] args)
    {
        String role = System.getProperty("role");
        String operation = System.getProperty("operation");

        if (role == null || operation == null)
        {
            System.err.println("Run with -Doperation={start|stop} -Drole={coordinator|worker}");
            System.exit(1);
        }

        if (!role.equalsIgnoreCase("coordinator") && !role.equalsIgnoreCase("worker")||
                !operation.equalsIgnoreCase("start") && !operation.equalsIgnoreCase("stop"))
        {
            System.err.println("Run with -Doperation={start|stop} -Drole={coordinator|worker}");
            System.exit(1);
        }

        String varDir = ConfigFactory.Instance().getProperty("pixels.var.dir");
        if (!varDir.endsWith("/") && !varDir.endsWith("\\"))
        {
            varDir += "/";
        }
        String lockFile = varDir + "pixels." + role.toLowerCase() + ".lock";

        if (operation.equalsIgnoreCase("start"))
        {
            // this is the main daemon.
            System.out.println("Starting daemon of " + role + "...");
            Daemon mainDaemon = new Daemon();
            mainDaemon.setup(lockFile);

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
            boolean autoScalingEnabled = Boolean.parseBoolean(config.getProperty("vm.auto.scaling.enabled"));
            boolean sinkEnabled = Boolean.parseBoolean(config.getProperty("sink.enabled"));

            if (role.equalsIgnoreCase("coordinator"))
            {
                int metadataServerPort = Integer.parseInt(config.getProperty("metadata.server.port"));
                int transServerPort = Integer.parseInt(config.getProperty("trans.server.port"));
                int queryScheduleServerPort = Integer.parseInt(config.getProperty("query.schedule.server.port"));
                int scalingMetricsServerPort = Integer.parseInt(config.getProperty("scaling.metrics.server.port"));

                try
                {
                    // start heartbeat coordinator
                    HeartbeatCoordinator heartbeatCoordinator = new HeartbeatCoordinator();
                    container.addServer("heartbeat_coordinator", heartbeatCoordinator);
                    // start metadata server
                    MetadataServer metadataServer = new MetadataServer(metadataServerPort);
                    container.addServer("metadata", metadataServer);
                    // start transaction server
                    TransServer transServer = new TransServer(transServerPort);
                    container.addServer("transaction", transServer);
                    // start query schedule server
                    QueryScheduleServer queryScheduleServer = new QueryScheduleServer(queryScheduleServerPort);
                    container.addServer("query_schedule", queryScheduleServer);

                    if(sinkEnabled) {
                        // start sink server
                        SinkServer sinkServer = new SinkServer();
                        container.addServer("sink", sinkServer);
                    }

                    if (autoScalingEnabled) {
                        // start monitor server
                        ScalingMetricsServer scalingMetricsServer = new ScalingMetricsServer(scalingMetricsServerPort);
                        container.addServer("scaling_metrics", scalingMetricsServer);
                    }
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
            } else
            {
                boolean metricsServerEnabled = Boolean.parseBoolean(
                        ConfigFactory.Instance().getProperty("metrics.server.enabled"));
                int retinaServerPort = Integer.parseInt(config.getProperty("retina.server.port"));

                try
                {
                    // start heartbeat worker
                    HeartbeatWorker heartbeatWorker = new HeartbeatWorker();
                    container.addServer("heartbeat_worker", heartbeatWorker);
                    // start metrics server and cache worker on worker node
                    if (metricsServerEnabled)
                    {
                        MetricsServer metricsServer = new MetricsServer();
                        container.addServer("metrics", metricsServer);
                    }
                    if (cacheEnabled)
                    {
                        CacheWorker cacheWorker = new CacheWorker();
                        container.addServer("cache_worker", cacheWorker);
                    }
                    // start retina server on worker node
                    RetinaServer retinaServer = new RetinaServer(retinaServerPort);
                    container.addServer("retina", retinaServer);
                } catch (Throwable e)
                {
                    log.error("failed to start worker", e);
                }
            }

            // The shutdown hook ensures the servers are shutdown graceful
            // if this main daemon is terminated by SIGTERM(15) signal.
            Runtime.getRuntime().addShutdownHook(new Thread(() ->
            {
                for (String serverName : container.getServerNames())
                {
                    // shutdown the server threads.
                    try
                    {
                        container.shutdownServer(serverName);
                    } catch (NoSuchServerException e)
                    {
                        log.error("error when stopping server threads", e);
                    }
                }
                for (int i = 60; i > 0; --i)
                {
                    try
                    {
                        boolean done = true;
                        for (String serverName : container.getServerNames())
                        {
                            if (container.checkServer(serverName, 0))
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
                log.info("all the servers are shutdown, bye...");
            }));

            // continue the main thread, start and check the server threads.
            // main thread will be terminated with the daemon thread.
            while (mainDaemon.isRunning())
            {
                try
                {
                    for (String serverName : container.getServerNames())
                    {
                        if (!container.checkServer(serverName))
                        {
                            container.startServer(serverName);
                        }
                    }
                    TimeUnit.SECONDS.sleep(1);
                } catch (Throwable e)
                {
                    log.error("error in the main loop of pixels daemon of " + role, e);
                    break;
                }
            }
            // the daemon is terminated.
        }
        else
        {
            System.out.println("Stopping daemon of " + role + "...");

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
                            splits[1].contains(PixelsWorker.class.getName()))
                    {
                        boolean roleFound = false;
                        boolean isStartOperation = false;
                        // get the role name of the target daemon (to be killing).
                        for (int i = 2; i < splits.length; ++i)
                        {
                            if (splits[i].contains("-Drole=" + role))
                            {
                                roleFound = true;
                            }
                            if (splits[i].contains("-Doperation=start"))
                            {
                                isStartOperation = true;
                            }
                            if (roleFound && isStartOperation)
                            {
                                break;
                            }
                        }
                        int pid = Integer.parseInt(splits[0]);
                        if (roleFound && isStartOperation)
                        {
                            System.out.println("killing " + role + ", pid (" + pid + ")");
                            // Terminate the daemon gracefully by sending SIGTERM(15) signal.
                            Runtime.getRuntime().exec("kill -15 " + pid);
                        }
                    }
                }
                reader.close();
                process.destroy();
            } catch (IOException e)
            {
                log.error("error when stopping pixels daemon of " + role, e);
            }
        }
    }
}
