package cn.edu.ruc.iir.pixels.load.cli;

import cn.edu.ruc.iir.pixels.load.util.LoaderUtil;
import com.facebook.presto.sql.parser.SqlParser;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import java.util.Scanner;

/**
 * pixels loader command line tool
 *
 * DDL -s /home/tao/software/data/pixels/DDL.txt -d pixels
 * DDL -s /home/tao/software/data/pixels/test30G_pixels/105/presto_ddl.sql -d pixels
 *
 * LOAD -f pixels -o hdfs://10.77.40.236:9000/pixels/test500G_text/ -d pixels -t testnull_pixels -n 300000
 * LOAD -f orc -o hdfs://10.77.40.236:9000/pixels/test500G_text/ -d pixels -t testnull_orc -n 300000
 *
 * <p>
 * LOAD -f pixels -o hdfs://dbiir01:9000/pixels/pixels/test_105/source -d pixels -t test_105 -n 300000 -r \t
 * </p>
 *
 */
public class LoaderCli
{
    public static void main(String[] args)
    {
        try {
            Loader loader = null;
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
                            "DDL\n" +
                            "LOAD\n");
                    System.out.println("{command} -h to show the usage of a command.\nexit / quit / -q to exit.\n");
                    continue;
                }

                String command = inputStr.trim().split("\\s+")[0].toUpperCase();

                if (command.equals("DDL"))
                {
                    ArgumentParser argumentParser = ArgumentParsers.newArgumentParser("DDL")
                            .defaultHelp(true);
                    argumentParser.addArgument("-s", "--schema_file").required(true)
                            .help("specify the path of schema file");
                    argumentParser.addArgument("-d", "--db_name").required(true)
                            .help("specify the name of database");
                    Namespace namespace;
                    try
                    {
                        namespace = argumentParser.parseArgs(inputStr.substring(command.length()).trim().split("\\s+"));
                    }
                    catch (ArgumentParserException e)
                    {
                        argumentParser.handleError(e);
                        continue;
                    }

                    SqlParser parser = new SqlParser();
                    String dbName = namespace.getString("db_name");
                    String schemaPath = namespace.getString("schema_file");
                    if (LoaderUtil.executeDDL(parser, dbName, schemaPath))
                    {
                        System.out.println("Executing command " + command + " successfully");
                    }
                    else
                    {
                        System.out.println("Executing command " + command + " unsuccessfully when adding table info");
                    }
                }

                if (command.equals("LOAD"))
                {
                    ArgumentParser argumentParser = ArgumentParsers.newArgumentParser("LOAD")
                            .defaultHelp(true);
                    argumentParser.addArgument("-f", "--format").required(true)
                            .help("Specify the format of files");
                    argumentParser.addArgument("-d", "--db_name").required(true)
                            .help("specify the name of database");
                    argumentParser.addArgument("-t", "--table_name").required(true)
                            .help("Specify the name of table");
                    argumentParser.addArgument("-o", "--original_data_path").required(true)
                            .help("specify the path of original data");
                    argumentParser.addArgument("-n", "--row_num").required(true)
                            .help("Specify the max number of rows to write in a file");
                    argumentParser.addArgument("-r", "--row_regex").required(true)
                            .help("Specify the split regex of each row in a file");
                    Namespace namespace;
                    try
                    {
                        namespace = argumentParser.parseArgs(inputStr.substring(command.length()).trim().split("\\s+"));
                    }
                    catch (ArgumentParserException e)
                    {
                        argumentParser.handleError(e);
                        continue;
                    }

                    String format = namespace.getString("format");
                    String dbName = namespace.getString("db_name");
                    String tableName = namespace.getString("table_name");
                    String originalDataPath = namespace.getString("original_data_path");
                    int rowNum = Integer.parseInt(namespace.getString("row_num"));
                    String regex = namespace.getString("row_regex");

                    if (format.equalsIgnoreCase("orc"))
                    {
                        loader = new ORCLoader(originalDataPath, dbName, tableName, rowNum, regex);
                    }
                    if (format.equalsIgnoreCase("pixels"))
                    {
                        loader = new PixelsLoader(originalDataPath, dbName, tableName, rowNum, regex);
                    }

                    if (loader != null)
                    {
                        if (loader.load())
                        {
                            System.out.println("Executing command " + command + " successfully");
                        }
                        else
                        {
                            System.out.println("Executing command " + command + " unsuccessfully when loading data");
                        }
                    }
                }

                if (!command.equals("DDL") && !command.equals("LOAD"))
                {
                    System.out.println("Command error");
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

}
