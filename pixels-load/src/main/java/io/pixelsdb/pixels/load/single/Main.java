/*
 * Copyright 2018 PixelsDB.
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
 * License along with Foobar.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.load.single;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import java.util.Scanner;

/**
 * pixels loader command line tool
 *
 * <p>
 * LOAD -f pixels -o hdfs://dbiir01:9000/pixels/pixels/test_105/source -d pixels -t test_105 -n 300000 -r \t
 * </p>
 *
 */
public class Main
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
                            "LOAD\n");
                    System.out.println("{command} -h to show the usage of a command.\nexit / quit / -q to exit.\n");
                    continue;
                }

                String command = inputStr.trim().split("\\s+")[0].toUpperCase();

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

                if (!command.equals("LOAD"))
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
