package io.pixelsdb.pixels.worker.vhive;

import org.apache.commons.cli.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Main {
    // here are the default values if not specified in args
    private static final Logger log = LogManager.getLogger(Main.class);
    private static final int PORT = 50051;

    public static void main(String[] args) {
        Options options = new Options();

        options.addOption(Option.builder("p")
                .longOpt("port")
                .hasArg(true)
                .desc("GRPC port ([REQUIRED] or use --port)")
                .required(false)
                .build());

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);

            String port = cmd.getOptionValue("port", Integer.toString(PORT));
            WorkerServer server = new WorkerServer(Integer.parseInt(port));
            server.run();
            log.info("Server start running");
        } catch (ParseException pe) {
            System.out.println("Error parsing command-line arguments!");
            System.out.println("Please, follow the instructions below:");
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("Log messages to sequence diagrams converter", options);
            System.exit(1);
        }
    }
}
