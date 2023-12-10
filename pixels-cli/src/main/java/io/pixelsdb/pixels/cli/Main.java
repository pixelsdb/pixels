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
package io.pixelsdb.pixels.cli;

import io.pixelsdb.pixels.cli.executor.*;
import io.pixelsdb.pixels.common.physical.Storage;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import java.sql.*;
import java.util.Properties;
import java.util.Scanner;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * @author tao
 * @author hank
 * @create in 2018-10-30 11:07
 **/

/**
 * pixels loader command line tool
 * <p>
 * LOAD -f pixels -o s3://text-105/source -s pixels -t test_105 -n 275000 -r \t -c 16 -l s3://pixels-105/v-0-order
 * [-l] is optional, its default value is the orderPath of the last writable layout of the table.
 *
 * <br>This should be run under root user to execute cache cleaning commands
 * <p>
 * QUERY -w /home/iir/opt/pixels/1187_dedup_query.txt -l /home/iir/opt/pixels/pixels_duration_1187_v_1_compact_cache_2020.01.10-2.csv -c /home/iir/opt/presto-server/sbin/drop-caches.sh
 * </p>
 * <p> Local
 * QUERY -w /home/tao/software/station/bitbucket/105_dedup_query.txt -l /home/tao/software/station/bitbucket/pixels_duration_local.csv
 * </p>
 * <p>
 * COPY -p .pxl -s hdfs://dbiir27:9000/pixels/pixels/test_105/v_1_order -d hdfs://dbiir27:9000/pixels/pixels/test_105/v_1_order -n 3 -c 3
 * </p>
 * <p>
 * COMPACT -s pixels -t test_105 -n yes -c 8
 * </p>
 * <p>
 * STAT -s tpch -t region
 * </p>
 */
public class Main
{
    public static void main(String args[])
    {
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
                        "LOAD\n" +
                        "QUERY\n" +
                        "COPY\n" +
                        "COMPACT\n" +
                        "STAT");
                System.out.println("{command} -h to show the usage of a command.\nexit / quit / -q to exit.\n");
                continue;
            }

            String command = inputStr.trim().split("\\s+")[0].toUpperCase();

            if (command.equals("LOAD"))
            {
                ArgumentParser argumentParser = ArgumentParsers.newArgumentParser("Pixels ETL LOAD")
                        .defaultHelp(true);

                argumentParser.addArgument("-o", "--original_data_path").required(true)
                        .help("specify the path of original data");
                argumentParser.addArgument("-s", "--schema").required(true)
                        .help("specify the name of database");
                argumentParser.addArgument("-t", "--table").required(true)
                        .help("specify the name of table");
                argumentParser.addArgument("-n", "--row_num").required(true)
                        .help("specify the max number of rows to write in a file");
                argumentParser.addArgument("-r", "--row_regex").required(true)
                        .help("specify the split regex of each row in a file");
                argumentParser.addArgument("-c", "--consumer_thread_num").setDefault("4").required(true)
                        .help("specify the number of consumer threads used for data generation");
                argumentParser.addArgument("-e", "--encoding_level").setDefault("2")
                        .help("specify the encoding level for data loading");
                argumentParser.addArgument("-p", "--nulls_padding").setDefault(false)
                        .help("specify whether nulls padding is enabled");
                argumentParser.addArgument("-l", "--loading_data_paths")
                        .help("specify the paths where the data is loaded into");

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
                    LoadExecutor loadExecutor = new LoadExecutor();
                    loadExecutor.execute(ns, command);
                } catch (Exception e)
                {
                    e.printStackTrace();
                }
            }

            if (command.equals("QUERY"))
            {
                ArgumentParser argumentParser = ArgumentParsers.newArgumentParser("Pixels QUERY")
                        .defaultHelp(true);

                argumentParser.addArgument("-r", "--repeat").required(true)
                        .help("specify whether run the queries in the workload file repeatedly");
                argumentParser.addArgument("-w", "--workload").required(true)
                        .help("specify the path of workload file");
                argumentParser.addArgument("-l", "--log").required(true)
                        .help("specify the path of query log files");
                argumentParser.addArgument("-c", "--cache").required(false)
                        .help("specify the command of dropping cache");



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
                    QueryExecutor queryExecutor = new QueryExecutor();
                    queryExecutor.execute(ns, command);
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
                        .help("specify the postfix of files to be copied");
                argumentParser.addArgument("-s", "--source").required(true)
                        .help("specify the source directory");
                argumentParser.addArgument("-d", "--destination").required(true)
                        .help("specify the destination directory");
                argumentParser.addArgument("-n", "--number").required(true)
                        .help("specify the number of copies");
                argumentParser.addArgument("-c", "--concurrency")
                        .setDefault("4").required(true)
                        .help("specify the number of threads used for data compaction");

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
                    CopyExecutor copyExecutor = new CopyExecutor();
                    copyExecutor.execute(ns, command);
                }
                catch (Exception e)
                {
                    e.printStackTrace();
                }
            }

            if (command.equals("COMPACT"))
            {
                ArgumentParser argumentParser = ArgumentParsers.newArgumentParser("Pixels ETL COMPACT")
                        .defaultHelp(true);

                argumentParser.addArgument("-s", "--schema").required(true)
                        .help("specify the name of schema.");
                argumentParser.addArgument("-t", "--table").required(true)
                        .help("specify the name of table.");
                argumentParser.addArgument("-n", "--naive").required(true)
                        .help("specify whether or not to create naive compact layout.");
                argumentParser.addArgument("-c", "--concurrency")
                        .setDefault("4").required(true)
                        .help("specify the number of threads used for data compaction");

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
                    CompactExecutor compactExecutor = new CompactExecutor();
                    compactExecutor.execute(ns, command);
                }
                catch (Exception e)
                {
                    e.printStackTrace();
                }
            }

            if (command.equals("STAT"))
            {
                ArgumentParser argumentParser = ArgumentParsers.newArgumentParser("Pixels Update Statistics")
                        .defaultHelp(true);

                argumentParser.addArgument("-s", "--schema").required(true)
                        .help("specify the schema name");
                argumentParser.addArgument("-t", "--table").required(true)
                        .help("specify the table name");

                Namespace ns = null;
                try
                {
                    ns = argumentParser.parseArgs(inputStr.substring(command.length()).trim().split("\\s+"));
                } catch (ArgumentParserException e)
                {
                    argumentParser.handleError(e);
                    System.out.println("Pixels STAT.");
                    System.exit(0);
                }

                try
                {
                    StatExecutor statExecutor = new StatExecutor();
                    statExecutor.execute(ns, command);
                }
                catch (Exception e)
                {
                    e.printStackTrace();
                }
            }

            if (!command.equals("QUERY") &&
                    !command.equals("LOAD") &&
                    !command.equals("COPY") &&
                    !command.equals("COMPACT") &&
                    !command.equals("STAT"))
            {
                System.out.println("Command error");
            }
        }
        // Use exit to terminate other threads and invoke the shutdown hooks.
        System.exit(0);
    }

    public static long executeSQL(String jdbcUrl, Properties jdbcProperties, String sql, String id)
    {
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

    /**
     * Check if the order or compact path from pixels metadata is valid.
     * @param paths the order or compact pathw from pixels metadata.
     */
    public static void validateOrderOrCompactPath(String[] paths)
    {
        requireNonNull(paths, "paths is null");
        checkArgument(paths.length > 0, "path must contain at least one valid directory");
        try
        {
            Storage.Scheme firstScheme = Storage.Scheme.fromPath(paths[0]);
            for (int i = 1; i < paths.length; ++i)
            {
                Storage.Scheme scheme = Storage.Scheme.fromPath(paths[i]);
                checkArgument(firstScheme.equals(scheme),
                        "all the directories in the path must have the same storage scheme");
            }
        } catch (Throwable e)
        {
            throw new RuntimeException("failed to parse storage scheme from path", e);
        }
    }
}