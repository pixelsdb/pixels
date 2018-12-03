package cn.edu.ruc.iir.pixels.load.multi;

import cn.edu.ruc.iir.pixels.common.physical.FSFactory;
import cn.edu.ruc.iir.pixels.common.utils.ConfigFactory;
import cn.edu.ruc.iir.pixels.presto.evaluator.PrestoEvaluator;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.sql.*;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.load.multi
 * @ClassName: Main
 * @Description:
 * @author: tao
 * @date: Create in 2018-10-30 11:07
 **/

/**
 * pixels loader command line tool
 * <p>
 * LOAD -f pixels -o hdfs://dbiir10:9000/pixels/pixels/test_1187/source -d pixels -t test_1187 -n 25000 -r \t -c 16
 * -p false [optional, default false]
 * </p>
 *
 * <br>This shall be run under root user to execute cache cleaning commands
 * <p>
 * QUERY -t pixels -w /home/iir/opt/pixels/105_dedup_query.txt -l /home/iir/opt/pixels/pixels_duration.csv -c /home/iir/opt/presto-server/sbin/drop-caches.sh
 * </p>
 * <p> Local
 * QUERY -t pixels -w /home/tao/software/station/bitbucket/105_dedup_query.txt -l /home/tao/software/station/bitbucket/pixels_duration_local.csv
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

                Namespace ns = null;
                try
                {
                    ns = argumentParser.parseArgs(inputStr.substring(command.length()).trim().split("\\s+"));
                } catch (ArgumentParserException e)
                {
                    argumentParser.handleError(e);
                    System.out.println("Pixels ETL (link).");
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

                    int threadNum = Integer.valueOf(ns.getString("consumer_thread_num"));
                    boolean producer = ns.getBoolean("producer");

                    BlockingQueue<Path> fileQueue = null;
                    ConfigFactory configFactory = ConfigFactory.Instance();
                    FSFactory fsFactory = FSFactory.Instance(configFactory.getProperty("hdfs.config.dir"));

                    if (format != null)
                    {
                        config = new Config(dbName, tableName, rowNum, regex, format);
                    }

                    if (producer && config != null)
                    {
                        // todo the producer option is true, means that the producer is dynamic

                    } else if (!producer && config != null)
                    {
                        // source already exist, producer option is false, add list of source to the queue
                        List<Path> hdfsList = fsFactory.listFiles(originalDataPath);
                        fileQueue = new LinkedBlockingDeque<>(hdfsList);

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
                    System.out.println("Pixels QUERY (link).");
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
//        String user = instance.getProperty("presto.user");
                        String password = instance.getProperty("presto.password");
                        String ssl = instance.getProperty("presto.ssl");
                        String jdbc = instance.getProperty("presto.pixels.jdbc.url");
                        if (type.equalsIgnoreCase("orc")) {
                            jdbc = instance.getProperty("presto.orc.jdbc.url");
                        }

                        if (!password.equalsIgnoreCase("null")) {
                            properties.setProperty("password", password);
                        }
                        properties.setProperty("SSL", ssl);

                        try (BufferedReader workloadReader = new BufferedReader(new FileReader(workload));
                             BufferedWriter timeWriter = new BufferedWriter(new FileWriter(log))) {
                            timeWriter.write("query id,id,duration(ms)\n");
                            timeWriter.flush();
                            String line;
                            int i = 0;
                            String defaultUser = null;
                            while ((line = workloadReader.readLine()) != null) {
                                if (!line.contains("SELECT")) {
                                    defaultUser = line;
                                    properties.setProperty("user", type + "_" + defaultUser);
                                } else {
                                    long cost = executeSQL(jdbc, properties, line, defaultUser);
                                    timeWriter.write(defaultUser + "," + i + "," + cost + "\n");
                                    i++;
                                    long start = System.currentTimeMillis();
                                    Thread.sleep(15 * 1000);
                                    if (i % 10 == 0) {
                                        timeWriter.flush();
                                        System.out.println(i);
                                    }
                                    System.out.println(i + "," + cost + "ms" + ",wait:" + (System.currentTimeMillis() - start) + "ms\n");
                                }

                                if(cache != null){
                                    ProcessBuilder processBuilder = new ProcessBuilder(cache);
                                    Process process = processBuilder.start();
                                    process.waitFor();
                                    Thread.sleep(1000);
                                }
                            }
                            timeWriter.flush();
                        } catch (Exception e) {
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

            if (!command.equals("QUERY") && !command.equals("LOAD"))
            {
                System.out.println("Command error");
            }
        }

    }

    public static long executeSQL(String jdbcUrl, Properties jdbcProperties, String sql, String id) {
        long start = 0L, end = 0L;
        try (Connection connection = DriverManager.getConnection(jdbcUrl, jdbcProperties)) {
            Statement statement = connection.createStatement();
            start = System.currentTimeMillis();
            ResultSet resultSet = statement.executeQuery(sql);
            resultSet.next();
            end = System.currentTimeMillis();
            statement.close();
        } catch (SQLException e) {
            System.out.println("SQL: " + id + "\n" + sql);
            System.out.println("Error msg: " + e.getMessage());
        }
        return end - start;
    }

}