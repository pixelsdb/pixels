package cn.edu.ruc.iir.pixels.daemon;

import cn.edu.ruc.iir.pixels.common.utils.ConfigFactory;
import cn.edu.ruc.iir.pixels.common.utils.DBUtil;
import cn.edu.ruc.iir.pixels.daemon.cache.CacheManager;
import cn.edu.ruc.iir.pixels.daemon.metadata.MetadataServer;
import cn.edu.ruc.iir.pixels.daemon.metric.MetricsServer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.TimeUnit;

/**
 * java -Dio.netty.leakDetection.level=advanced -Drole=main -jar pixels-daemon-0.1.0-SNAPSHOT-full.jar metadata
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
                    (args[0].equalsIgnoreCase("metadata") || args[0].equalsIgnoreCase("datanode")))
            {
                // this is the main daemon
                System.out.println("starting main daemon...");
                Daemon guardDaemon = new Daemon();
                String[] guardCmd = {"java", "-Drole=guard", "-jar", daemonJarPath, args[0]};
                guardDaemon.setup(mainFile, guardFile, guardCmd);
                Thread daemonThread = new Thread(guardDaemon);
                daemonThread.setName("main daemon thread");
                daemonThread.setDaemon(true);
                daemonThread.start();

                ServerContainer container = new ServerContainer();

                ConfigFactory config = ConfigFactory.Instance();
                int port = Integer.valueOf(config.getProperty("metadata.server.port"));
                MetadataServer metadataServer = new MetadataServer(port);

                CacheManager cacheServer = new CacheManager();

                MetricsServer metricsServer = new MetricsServer();

                if (args[0].equalsIgnoreCase("metadata"))
                {
                    // start metadata
                    container.addServer("metadata", metadataServer);
                }
                else
                {
                    // start data node
                    container.addServer("cache", cacheServer);
                    container.addServer("metrics", metricsServer);
                }

                // continue the main thread
                while (true)
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
                        if (args[0].equalsIgnoreCase("metadata"))
                        {
                            // close the metadata database connection.
                            DBUtil.Instance().close();
                        }
                    }
                }


            } else if (role.equalsIgnoreCase("guard") && args.length == 1 &&
                    (args[0].equalsIgnoreCase("metadata") || args[0].equalsIgnoreCase("datanode")))
            {
                // this is the guard daemon
                System.out.println("starting guard daemon...");
                Daemon guardDaemon = new Daemon();
                String[] guardCmd = {"java", "-Drole=main", "-jar", daemonJarPath, args[0]};
                guardDaemon.setup(guardFile, mainFile, guardCmd);
                guardDaemon.run();
            } else if (role.equalsIgnoreCase("kill"))
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
                        if (splits[1].contains(jarName))
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
                            // TODO: this is not a gentle manner to terminate the daemon, we should notify the killing daemon to close database connection.
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
                System.err.println("Run with -Drole=[main,guard,kill], when role=main, there should be an args [metadata/datanode]");
            }
        }
        else
        {
            System.err.println("Run with -Drole=[main,guard,kill], when role=main, there should be an args [metadata/datanode]");
        }
    }
}
