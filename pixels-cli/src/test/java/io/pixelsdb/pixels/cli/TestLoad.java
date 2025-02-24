/*
 * Copyright 2025 PixelsDB.
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

import io.pixelsdb.pixels.cli.executor.LoadExecutor;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.junit.Test;

public class TestLoad
{
    @Test
    public void testLoad()
    {
        String inputStr = "LOAD -o file:///home/gengdy/data/tpch/1g/raw/customer -s tpch_1g -t customer -n 319150 -r \\| -c 1";
        String command = inputStr.trim().split("\\s+")[0].toUpperCase();

        if (command.equals("LOAD"))
        {
            ArgumentParser argumentParser = ArgumentParsers.newArgumentParser("Pixels ETL LOAD")
                    .defaultHelp(true);

            argumentParser.addArgument("-o", "--origin").required(true)
                    .help("specify the path of original data files");
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

            Namespace ns;
            try
            {
                ns = argumentParser.parseArgs(inputStr.substring(command.length()).trim().split("\\s+"));
            } catch (ArgumentParserException e)
            {
                argumentParser.handleError(e);
                return;
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
    }
}
