import org.apache.commons.cli.*;

/**
 * Created by khorm on 11/02/17.
 */
public class CLI {

    private final String USAGE = "main [OPTIONS] -mode <query-mode> -scale <scale-factor> [Path]";

    // modes
    private final String MODE_RANGE_ORDERS = "range-orders";
    private final String MODE_RANGE_LINEITEM = "range-lineitem";
    private final String MODE_JOIN_RANGE_ORDERS = "join-range-orders";
    private final String MODE_JOIN_RANGE_LINEITEM = "join-range-lineitem";
    private final String MODE_SAVE_PARQ = "save-parq";

    // flags
    private final String FLAG_HELP = "h";
    private final String FLAG_MODE = "m";
    private final String FLAG_SCALE = "s";
    private final String FLAG_CACHE = "C";
    private final String FLAG_PARQ = "P";



    private Options options;
    private HelpFormatter helpFormatter;
    private CommandLineParser commandLineParser;
    private CommandLine cmd;


    public CLI(String[] args) throws ParseException {


        options = new Options();
        addOptions();
        helpFormatter = new HelpFormatter();
        commandLineParser = new DefaultParser();
        cmd = this.parseCommandLine(args);




    }

    private void addOptions() {


        options.addOption(FLAG_HELP, "help", false, "print help message");
        options.addOption(FLAG_PARQ, "parquet", false, "NYI - read parq rather than CSV tables");
        options.addOption(FLAG_CACHE, "cache", false , "NYI - force cache the tables in memory");
        options.addOption(FLAG_SCALE, "scale", false , "NYI - tables scale factor");

        options.addOption(Option.builder(FLAG_MODE)
                .longOpt("mode")
                .hasArg(true)
                .required(true)
                .desc("query modes: \n"+
                        MODE_RANGE_ORDERS + " - incremental 10% query series on orders table\n" +
                        MODE_RANGE_LINEITEM + " - incremental 10% query series on lineitem table\n" +
                        MODE_JOIN_RANGE_ORDERS + " - join with incremental 10% query series on orders table\n" +
                        MODE_JOIN_RANGE_LINEITEM + " - join with incremental 10% query series on lineitem table\n" +
                        MODE_SAVE_PARQ + " - convert tables to parquett format")
                .build());



    }




    public boolean modeIsSaveAsParq(){
        return cmd.getOptionValue(FLAG_MODE).equals(MODE_SAVE_PARQ);
    }

    public boolean hasHelp(){
        return cmd.hasOption(FLAG_HELP);
    }


    public void printUsage(){
        helpFormatter.printHelp(USAGE, options);
    }


    public CommandLine parseCommandLine(String[] args) throws ParseException {
        return commandLineParser.parse(options, args);

    }

    public boolean modeIsRangeOrders() {
        return cmd.getOptionValue(FLAG_MODE).equals(MODE_RANGE_ORDERS);
    }

    public boolean modeIsRangeLineitem() {
        return cmd.getOptionValue(FLAG_MODE).equals(MODE_RANGE_LINEITEM);
    }

    public boolean modeIsJoinRangeOrders() {
        return cmd.getOptionValue(FLAG_MODE).equals(MODE_JOIN_RANGE_ORDERS);
    }

    public boolean modeIsJoinRangeLineitem() {
        return cmd.getOptionValue(FLAG_MODE).equals(MODE_JOIN_RANGE_LINEITEM);
    }

    public boolean hasScaleFactor() {
        return cmd.hasOption(FLAG_SCALE);
    }

    public int getScaleFactorValue() {
        return Integer.parseInt(cmd.getOptionValue(FLAG_SCALE));
    }

    public boolean hasPath() {
        return cmd.getArgList().size() > 0;
    }

    public String getFilePathValue() {
        return cmd.getArgList().get(0);
    }

    public boolean hasCache() {
        return cmd.hasOption(FLAG_CACHE);
    }
}
