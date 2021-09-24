/*
 * Copyright 2018-2019 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.load.multi;

import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.Compact;
import io.pixelsdb.pixels.common.metadata.domain.Layout;
import io.pixelsdb.pixels.common.physical.Status;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.common.utils.Constants;
import io.pixelsdb.pixels.common.utils.DateUtil;
import io.pixelsdb.pixels.core.compactor.CompactLayout;
import io.pixelsdb.pixels.core.compactor.PixelsCompactor;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author tao
 * @author hank
 * @date: Create in 2018-10-30 11:07
 **/

/**
 * pixels loader command line tool
 * <p>
 * LOAD -f pixels -o hdfs://dbiir27:9000/pixels/pixels/test_105/source -d pixels -t test_105 -n 220000 -r \t -c 16
 * -p false [optional, default false]
 * </p>
 * <p>
 * LOAD -f orc -o hdfs://dbiir10:9000/pixels/pixels/test_105/source -d pixels -t test_105 -n 220000 -r \t -c 16
 * -l hdfs://dbiir10:9000/pixels/pixels/test_105/v_0_order_orc/
 * </p>
 * [-l] is optional, assign a path not the 'OrderPath' in db(Defined in Config.java)
 *
 * <br>This should be run under root user to execute cache cleaning commands
 * <p>
 * QUERY -t pixels -w /home/iir/opt/pixels/1187_dedup_query.txt -l /home/iir/opt/pixels/pixels_duration_1187_v_1_compact_cache_2020.01.10-2.csv -c /home/iir/opt/presto-server/sbin/drop-caches.sh
 * </p>
 * <p> Local
 * QUERY -t pixels -w /home/tao/software/station/bitbucket/105_dedup_query.txt -l /home/tao/software/station/bitbucket/pixels_duration_local.csv
 * </p>
 * <p>
 * COPY -p .pxl -s hdfs://dbiir27:9000/pixels/pixels/test_105/v_1_order -d hdfs://dbiir27:9000/pixels/pixels/test_105/v_1_order -n 3
 * </p>
 * <p>
 * COMPACT -s pixels -t test_105 -l 3 -n yes
 * </p>
 */
public class Main
{
    public static void main(String args[])
    {
        Config config = null;
        Scanner scanner = new Scanner(System.in);
        String inputStr;

        while (true)
        {
            System.out.print("pixels> ");
            inputStr = scanner.nextLine().trim();

            if (inputStr.isEmpty() || inputStr.equals(";"))
            {
                continue;
            }

            if (inputStr.endsWith(";"))
            {
                inputStr = inputStr.substring(0, inputStr.length() - 1);
            }

            if (inputStr.equalsIgnoreCase("exit") || inputStr.equalsIgnoreCase("quit") ||
                    inputStr.equalsIgnoreCase("-q"))
            {
                System.out.println("Bye.");
                break;
            }

            if (inputStr.equalsIgnoreCase("help") || inputStr.equalsIgnoreCase("-h"))
            {
                System.out.println("Supported commands:\n" +
                        "LOAD\n"+
                        "QUERY\n");
                System.out.println("{command} -h to show the usage of a command.\nexit / quit / -q to exit.\n");
                continue;
            }

            String command = inputStr.trim().split("\\s+")[0].toUpperCase();

            if (command.equals("LOAD"))
            {
                ArgumentParser argumentParser = ArgumentParsers.newArgumentParser("Pixels ETL LOAD")
                        .defaultHelp(true);

                argumentParser.addArgument("-f", "--format").required(true)
                        .help("Specify the format of files");
                argumentParser.addArgument("-o", "--original_data_path").required(true)
                        .help("specify the path of original data");
                argumentParser.addArgument("-d", "--db_name").required(true)
                        .help("specify the name of database");
                argumentParser.addArgument("-t", "--table_name").required(true)
                        .help("Specify the name of table");
                argumentParser.addArgument("-n", "--row_num").required(true)
                        .help("Specify the max number of rows to write in a file");
                argumentParser.addArgument("-r", "--row_regex").required(true)
                        .help("Specify the split regex of each row in a file");
                argumentParser.addArgument("-c", "--consumer_thread_num").setDefault("4").required(true)
                        .help("specify the number of consumer threads used for data generation");
                argumentParser.addArgument("-p", "--producer").setDefault(false)
                        .help("specify the option of choosing producer");
                argumentParser.addArgument("-l", "--loading_data_path")
                        .help("specify the path of loading data");

                Namespace ns = null;
                try
                {
                    ns = argumentParser.parseArgs(inputStr.substring(command.length()).trim().split("\\s+"));
                } catch (ArgumentParserException e)
                {
                    argumentParser.handleError(e);
                    System.out.println("Pixels Load.");
                    System.exit(0);
                }

                try
                {
                    String format = ns.getString("format");
                    String dbName = ns.getString("db_name");
                    String tableName = ns.getString("table_name");
                    String originalDataPath = ns.getString("original_data_path");
                    int rowNum = Integer.parseInt(ns.getString("row_num"));
                    String regex = ns.getString("row_regex");
                    String loadingDataPath = ns.getString("loading_data_path");

                    int threadNum = Integer.valueOf(ns.getString("consumer_thread_num"));
                    boolean producer = ns.getBoolean("producer");

                    BlockingQueue<String> fileQueue;
                    Storage storage = StorageFactory.Instance().getStorage("hdfs");

                    if (format != null)
                    {
                        config = new Config(dbName, tableName, rowNum, regex, format, loadingDataPath);
                    }

                    if (producer && config != null)
                    {
                        // todo the producer option is true, means that the producer is dynamic

                    } else if (!producer && config != null)
                    {
                        // source already exist, producer option is false, add list of source to the queue
                        List<String> hdfsList = storage.listPaths(originalDataPath);
                        fileQueue = new LinkedBlockingQueue<>(hdfsList);

                        ConsumerGenerator instance = ConsumerGenerator.getInstance(threadNum);
                        long startTime = System.currentTimeMillis();

                        if (instance.startConsumer(fileQueue, config))
                        {
                            System.out.println("Executing command " + command + " successfully");
                        } else
                        {
                            System.out.println("Executing command " + command + " unsuccessfully when loading data");
                        }

                        long endTime = System.currentTimeMillis();
                        System.out.println("Files in Source " + originalDataPath + " is loaded into " + format + " format by " + threadNum + " threads in " + (endTime - startTime) / 1000 + "s.");

                    } else
                    {
                        System.out.println("Please input the producer option.");
                    }
                } catch (Exception e)
                {
                    e.printStackTrace();
                }
            }

            if (command.equals("QUERY"))
            {
                ArgumentParser argumentParser = ArgumentParsers.newArgumentParser("Pixels QUERY")
                        .defaultHelp(true);

                argumentParser.addArgument("-t", "--type").required(true)
                        .help("Specify the format of files");
                argumentParser.addArgument("-w", "--workload").required(true)
                        .help("specify the path of workload");
                argumentParser.addArgument("-l", "--log").required(true)
                        .help("Specify the path of log");
                argumentParser.addArgument("-c", "--cache")
                        .help("Specify the command of dropping cache");

                Namespace ns = null;
                try
                {
                    ns = argumentParser.parseArgs(inputStr.substring(command.length()).trim().split("\\s+"));
                } catch (ArgumentParserException e)
                {
                    argumentParser.handleError(e);
                    System.out.println("Pixels QUERY.");
                    System.exit(0);
                }

                try
                {
                    String type = ns.getString("type");
                    String workload = ns.getString("workload");
                    String log = ns.getString("log");
                    String cache = ns.getString("cache");

                    if (type != null && workload != null && log != null)
                    {
                        ConfigFactory instance = ConfigFactory.Instance();
                        Properties properties = new Properties();
                        // String user = instance.getProperty("presto.user");
                        String password = instance.getProperty("presto.password");
                        String ssl = instance.getProperty("presto.ssl");
                        String jdbc = instance.getProperty("presto.pixels.jdbc.url");
                        if (type.equalsIgnoreCase("orc"))
                        {
                            jdbc = instance.getProperty("presto.orc.jdbc.url");
                        }

                        if (!password.equalsIgnoreCase("null"))
                        {
                            properties.setProperty("password", password);
                        }
                        properties.setProperty("SSL", ssl);

                        try (BufferedReader workloadReader = new BufferedReader(new FileReader(workload));
                             BufferedWriter timeWriter = new BufferedWriter(new FileWriter(log)))
                        {
                            timeWriter.write("query id,id,duration(ms)\n");
                            timeWriter.flush();
                            String line;
                            int i = 0;
                            String defaultUser = null;
                            while ((line = workloadReader.readLine()) != null)
                            {
                                if (!line.contains("SELECT"))
                                {
                                    defaultUser = line;
                                    properties.setProperty("user", type + "_" + defaultUser);
                                } else
                                {
                                    if (cache != null)
                                    {
                                        long start = System.currentTimeMillis();
                                        ProcessBuilder processBuilder = new ProcessBuilder(cache);
                                        Process process = processBuilder.start();
                                        process.waitFor();
                                        Thread.sleep(1000);
                                        System.out.println("clear cache: " + (System.currentTimeMillis() - start) + "ms\n");
                                    }
                                    else
                                    {
                                        Thread.sleep(15 * 1000);
                                        System.out.println("wait 15000 ms\n");
                                    }

                                    long cost = executeSQL(jdbc, properties, line, defaultUser);
                                    timeWriter.write(defaultUser + "," + i + "," + cost + "\n");

                                    System.out.println(i + "," + cost + "ms");
                                    i++;
                                    if (i % 10 == 0)
                                    {
                                        timeWriter.flush();
                                        System.out.println(i);
                                    }

                                }
                            }
                            timeWriter.flush();
                        } catch (Exception e)
                        {
                            e.printStackTrace();
                        }
                    } else
                    {
                        System.out.println("Please input the parameters.");
                    }
                } catch (Exception e)
                {
                    e.printStackTrace();
                }
            }

            if (command.equals("COPY"))
            {
                ArgumentParser argumentParser = ArgumentParsers.newArgumentParser("Pixels ETL COPY")
                        .defaultHelp(true);

                argumentParser.addArgument("-p", "--postfix").required(true)
                        .help("Specify the postfix of files to be copied");
                argumentParser.addArgument("-s", "--source").required(true)
                        .help("specify the source directory");
                argumentParser.addArgument("-d", "--destination").required(true)
                        .help("Specify the destination directory");
                argumentParser.addArgument("-n", "--number").required(true)
                        .help("Specify the number of copies");

                Namespace ns = null;
                try
                {
                    ns = argumentParser.parseArgs(inputStr.substring(command.length()).trim().split("\\s+"));
                } catch (ArgumentParserException e)
                {
                    argumentParser.handleError(e);
                    System.out.println("Pixels COPY.");
                    System.exit(0);
                }

                try
                {
                    String postfix = ns.getString("postfix");
                    String source = ns.getString("source");
                    String destination = ns.getString("destination");
                    int n = Integer.parseInt(ns.getString("number"));

                    if (!destination.endsWith("/"))
                    {
                        destination += "/";
                    }

                    ConfigFactory configFactory = ConfigFactory.Instance();

                    Storage sourceStorage = StorageFactory.Instance().getStorage(source);
                    Storage destStorage = StorageFactory.Instance().getStorage(destination);

                    List<Status> files =  sourceStorage.listStatus(source);
                    long blockSize = Long.parseLong(configFactory.getProperty("block.size")) * 1024l * 1024l;
                    short replication = Short.parseShort(configFactory.getProperty("block.replication"));

                    for (int i = 0; i < n; ++i)
                    {
                        for (Status s : files)
                        {
                            String sourceName = s.getName();
                            String destName = destination +
                                    sourceName.substring(0, sourceName.indexOf(postfix)) +
                                    "_copy_" + DateUtil.getCurTime() + postfix;
                            String dest = destName;
                            DataInputStream inputStream = sourceStorage.open(s.getPath());
                            DataOutputStream outputStream = destStorage.create(dest, false,
                                    Constants.HDFS_BUFFER_SIZE, replication, blockSize);
                            IOUtils.copyBytes(inputStream, outputStream, Constants.HDFS_BUFFER_SIZE, true);
                        }
                    }
                }
                catch (IOException e)
                {
                    e.printStackTrace();
                }
            }

            if (command.equals("COMPACT"))
            {
                ArgumentParser argumentParser = ArgumentParsers.newArgumentParser("Pixels ETL COPY")
                        .defaultHelp(true);

                argumentParser.addArgument("-s", "--schema").required(true)
                        .help("Specify the name of schema.");
                argumentParser.addArgument("-t", "--table").required(true)
                        .help("specify the name of table.");
                argumentParser.addArgument("-l", "--layout").required(true)
                        .help("Specify the id of the layout to compact.");
                argumentParser.addArgument("-n", "--naive").required(true)
                        .help("Specify whether or not to create naive compact layout.");

                Namespace ns = null;
                try
                {
                    ns = argumentParser.parseArgs(inputStr.substring(command.length()).trim().split("\\s+"));
                } catch (ArgumentParserException e)
                {
                    argumentParser.handleError(e);
                    System.out.println("Pixels Compact.");
                    System.exit(0);
                }

                try
                {
                    String schema = ns.getString("schema");
                    String table = ns.getString("table");
                    int layoutId = Integer.parseInt(ns.getString("layout"));
                    String naive = ns.getString("naive");

                    String metadataHost = ConfigFactory.Instance().getProperty("metadata.server.host");
                    int metadataPort = Integer.parseInt(ConfigFactory.Instance().getProperty("metadata.server.port"));

                    // get compact layout
                    MetadataService metadataService = new MetadataService(metadataHost, metadataPort);
                    List<Layout> layouts = metadataService.getLayouts(schema, table);
                    System.out.println("existing number of layouts: " + layouts.size());
                    Layout layout = null;
                    for (Layout layout1 : layouts)
                    {
                        if (layout1.getId() == layoutId)
                        {
                            layout = layout1;
                            break;
                        }
                    }

                    Compact compact = layout.getCompactObject();
                    int numRowGroupInBlock = compact.getNumRowGroupInBlock();
                    CompactLayout compactLayout;
                    if (naive.equalsIgnoreCase("yes") || naive.equalsIgnoreCase("y"))
                    {
                        compactLayout = CompactLayout.buildNaive(
                                compact.getNumRowGroupInBlock(), compact.getNumColumn());
                    }
                    else
                    {
                        compactLayout = CompactLayout.fromCompact(compact);
                    }

                    // get input file paths
                    ConfigFactory configFactory = ConfigFactory.Instance();
                    Storage storage = StorageFactory.Instance().getStorage("hdfs"); // TODO: support other storage type.
                    long blockSize = Long.parseLong(configFactory.getProperty("block.size")) * 1024l * 1024l;
                    short replication = Short.parseShort(configFactory.getProperty("block.replication"));
                    List<Status> statuses = storage.listStatus(layout.getOrderPath());

                    // compact
                    for (int i = 0; i + numRowGroupInBlock < statuses.size(); i+=numRowGroupInBlock)
                    {
                        List<String> sourcePaths = new ArrayList<>();
                        for (int j = 0; j < numRowGroupInBlock; ++j)
                        {
                            //System.out.println(statuses[i+j].getPath().toString());
                            sourcePaths.add(statuses.get(i+j).getPath());
                        }

                        long start = System.currentTimeMillis();

                        String filePath = layout.getCompactPath() + (layout.getCompactPath().endsWith("/") ? "" : "/") +
                                DateUtil.getCurTime() +
                                ".compact.pxl";
                        PixelsCompactor pixelsCompactor =
                                PixelsCompactor.newBuilder()
                                        .setSourcePaths(sourcePaths)
                                        .setCompactLayout(compactLayout)
                                        .setStorage(storage)
                                        .setFilePath(filePath)
                                        .setBlockSize(blockSize)
                                        .setReplication(replication)
                                        .setBlockPadding(false)
                                        .build();
                        pixelsCompactor.compact();
                        pixelsCompactor.close();

                        System.out.println(((System.currentTimeMillis() - start) / 1000.0) + " s for [" + filePath + "]");
                    }
                } catch (MetadataException e)
                {
                    e.printStackTrace();
                } catch (FileNotFoundException e)
                {
                    e.printStackTrace();
                } catch (IOException e)
                {
                    e.printStackTrace();
                }
            }

            if (!command.equals("QUERY") &&
                    !command.equals("LOAD") &&
                    !command.equals("COPY") &&
                    !command.equals("COMPACT"))
            {
                System.out.println("Command error");
            }
        }

    }

    public static long executeSQL(String jdbcUrl, Properties jdbcProperties, String sql, String id) {
        long start = 0L, end = 0L;
        try (Connection connection = DriverManager.getConnection(jdbcUrl, jdbcProperties))
        {
            Statement statement = connection.createStatement();
            start = System.currentTimeMillis();
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {}
            end = System.currentTimeMillis();
            resultSet.close();
            statement.close();
        } catch (SQLException e)
        {
            System.out.println("SQL: " + id + "\n" + sql);
            System.out.println("Error msg: " + e.getMessage());
        }
        return end - start;
    }

}