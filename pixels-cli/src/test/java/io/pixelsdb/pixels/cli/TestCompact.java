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

import io.pixelsdb.pixels.cli.executor.CompactExecutor;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.junit.Test;

public class TestCompact
{
    @Test
    public void testCompact()
    {
        String inputStr = "COMPACT -s tpch_1g -t customer -n no -c 2";
        String command = inputStr.trim().split("\\s+")[0].toUpperCase();

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
            CompactExecutor compactExecutor = new CompactExecutor();
            compactExecutor.execute(ns, command);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}
