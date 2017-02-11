import org.apache.commons.cli.*;

/**
 * Created by khorm on 11/02/17.
 */
public class CLI {

    private final String USAGE = "main [OPTIONS] -mode <query-mode> -scale <scale-factor> [Path]";

    private Options options;
    private String[] args;

    private HelpFormatter helpFormatter;
    private CommandLineParser commandLineParser;


    public CLI(String[] args){


        options = new Options();
        addOptions();
        helpFormatter = new HelpFormatter();
        commandLineParser = new DefaultParser();




    }

    private void addOptions() {


        options.addOption("h", "help", false, "print help message");
        options.addOption("P", false, "NYI - read parq rather than CSV tables");
        options.addOption("c", "cache", false , "NYI - force cache the tables in memory");
        options.addOption("s", "scale", false , "NYI - tables scale factor");

        options.addOption(Option.builder("m")
                .longOpt("mode")
                .hasArg(true)
                .required(true)
                .desc("query modes: \n"+
                        "range-orders - incremental 10% query series on orders table\n" +
                        "range-lineitem - incremental 10% query series on lineitem table\n" +
                        "join-range-orders - join with incremental 10% query series on orders table\n" +
                        "join-range-lineitem - join with incremental 10% query series on lineitem table\n" +
                        "save-parq - convert tables to parquett format")
                .build());



    }


    public void Usage(){
        helpFormatter.printHelp(USAGE, options);
    }


    public CommandLine parseCommandLine() throws ParseException {

        return commandLineParser.parse(options, args);

    }
}
