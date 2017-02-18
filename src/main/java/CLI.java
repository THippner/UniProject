import org.apache.commons.cli.*;

/**
 * Created by khorm on 11/02/17.
 */
public class CLI {

    private final String USAGE = "main [OPTIONS] -mode <query-mode> -scale <scale-factor> [Path]";

    // modes
    private final String MODE_RANGE_ORDERS = "range-orders";
    private final String MODE_RANGE_ORDERS_ALIAS = "ro";
    private final String MODE_RANGE_LINEITEM = "range-lineitem";
    private final String MODE_RANGE_LINEITEM_ALIAS = "rl";
    private final String MODE_JOIN_RANGE_ORDERS = "join-range-orders";
    private final String MODE_JOIN_RANGE_ORDERS_ALIAS = "jro";
    private final String MODE_JOIN_RANGE_LINEITEM = "join-range-lineitem";
    private final String MODE_JOIN_RANGE_LINEITEM_ALIAS = "jrl";
    private final String MODE_SAVE_PARQ = "save-parquet";
    private final String MODE_SAVE_PARQ_ALIAS = "sp";
    private final String MODE_DB_TEST = "database-test";
    private final String MODE_DB_TEST_ALIAS = "dt";

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
        commandLineParser = new BasicParser();
        cmd = this.parseCommandLine(args);




    }

    @SuppressWarnings("static-access")
    private void addOptions() {


        options.addOption(FLAG_HELP, "help", false, "print help message");
        options.addOption(FLAG_PARQ, "parquet", false, "NYI - read parq rather than CSV tables");
        options.addOption(FLAG_CACHE, "cache", false , "NYI - force cache the tables in memory");
        options.addOption(FLAG_SCALE, "scale", true , "TPCH db scale factor for tables");

        options.addOption(OptionBuilder
                .withLongOpt("mode")
                .hasArg(true)
                //.isRequired(true)
                .withDescription("query modes: \n"+
                        MODE_RANGE_ORDERS + ", " + MODE_RANGE_ORDERS_ALIAS + " - incremental 10% query series on orders table\n" +
                        MODE_RANGE_LINEITEM + ", " + MODE_RANGE_LINEITEM_ALIAS + " - incremental 10% query series on lineitem table\n" +
                        MODE_JOIN_RANGE_ORDERS + ", "+ MODE_JOIN_RANGE_ORDERS_ALIAS + " - join with incremental 10% query series on orders table\n" +
                        MODE_JOIN_RANGE_LINEITEM + ", "+ MODE_JOIN_RANGE_LINEITEM_ALIAS + " - join with incremental 10% query series on lineitem table\n" +
                        MODE_SAVE_PARQ + ", "+ MODE_SAVE_PARQ_ALIAS + " - convert tables to parquett format\n" +
                        MODE_DB_TEST + ", " + MODE_DB_TEST_ALIAS + " - single count query to both tables")
                .create(FLAG_MODE));



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


    // ----------------------------------------------------------------- MODES
    public boolean modeIsRangeOrders() {

        String mode = cmd.getOptionValue(FLAG_MODE);
        return (mode.equals(MODE_RANGE_ORDERS) || mode.equals(MODE_RANGE_ORDERS_ALIAS));
    }

    public boolean modeIsRangeLineitem() {

        String mode = cmd.getOptionValue(FLAG_MODE);
        return (mode.equals(MODE_RANGE_LINEITEM) || mode.equals(MODE_RANGE_LINEITEM_ALIAS));
    }

    public boolean modeIsJoinRangeOrders() {

        String mode = cmd.getOptionValue(FLAG_MODE);
        return (mode.equals(MODE_JOIN_RANGE_ORDERS) || mode.equals(MODE_JOIN_RANGE_ORDERS_ALIAS));
    }

    public boolean modeIsJoinRangeLineitem() {

        String mode = cmd.getOptionValue(FLAG_MODE);
        return (mode.equals(MODE_JOIN_RANGE_LINEITEM) || mode.equals(MODE_JOIN_RANGE_LINEITEM_ALIAS));
    }

    public boolean modeIsSaveAsParq(){

        String mode = cmd.getOptionValue(FLAG_MODE);
        return (mode.equals(MODE_SAVE_PARQ) || mode.equals(MODE_SAVE_PARQ_ALIAS));
    }

    public boolean modeIsDatabaseTest(){

        String mode = cmd.getOptionValue(FLAG_MODE);
        return (mode.equals(MODE_DB_TEST) || mode.equals(MODE_DB_TEST_ALIAS));
    }






    public boolean hasScaleFactor() {
        return cmd.hasOption(FLAG_SCALE);
    }
    public boolean hasParquetFlag() { return cmd.hasOption(FLAG_PARQ); }

    public int getScaleFactorValue() {
        return Integer.parseInt(cmd.getOptionValue(FLAG_SCALE));
    }



    public boolean hasPath() {
        return cmd.getArgList().size() > 0;    }

    public String getFilePathValue() { //TODO: Merge with has path
        return cmd.getArgList().get(0).toString();
    }

    public boolean hasCache() {
        return cmd.hasOption(FLAG_CACHE);
    }
}
