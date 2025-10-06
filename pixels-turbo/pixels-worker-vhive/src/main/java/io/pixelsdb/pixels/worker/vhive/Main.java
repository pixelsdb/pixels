/*
 * Copyright 2023 PixelsDB.
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
package io.pixelsdb.pixels.worker.vhive;

import org.apache.commons.cli.*;

public class Main
{
    // here are the default values if not specified in args
    private static final int PORT = 50051;

    public static void main(String[] args)
    {
        Options options = new Options();

        options.addOption(Option.builder("p")
                .longOpt("port")
                .hasArg(true)
                .desc("GRPC port ([REQUIRED] or use --port)")
                .required(false)
                .build());

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd;
        try
        {
            cmd = parser.parse(options, args);
            String port = cmd.getOptionValue("port", Integer.toString(PORT));
            final WorkerServer server = new WorkerServer(Integer.parseInt(port));
            Runtime.getRuntime().addShutdownHook(new Thread(server::shutdown));
            server.run();
        } catch (ParseException pe)
        {
            System.out.println("Error parsing command-line arguments!");
            System.out.println("Please, follow the instructions below:");
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("Log messages to sequence diagrams converter", options);
            System.exit(1);
        }
    }
}
