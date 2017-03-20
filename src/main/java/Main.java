/**
 * Tomasz Hippner
 * 2146437
 * 
 * Level 4 Project
 * School of Computing Science
 * 
 * University of Glasgow
 * 20/3/2017 
 */



import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.ParseException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.util.ArrayList;
import java.util.List;


/**
 * Main running class for the utility.
 */
public class Main {

    final static int REPEAT_QUERY_NUMBER = 10;

    final static int DATA_MULTIPL = 100000; // multiplier to make numbers smaller
    
    // numbers that correspond to 10% increments in size for each table
    final static int[] DATA_RANGE_FACTORS = {6, 12, 18, 24, 30, 36, 42, 48, 54}; 

    final static int DEFAULT_SCALE = 1;
    final static String DEFAULT_PATH = "/user/tomasz/db1/";




    public static void main(String[] args) {


        try {

            CLI cli = new CLI(args);
            List<SparkTable> tables = new ArrayList<SparkTable>();            
            String filePath = DEFAULT_PATH;
            int multipliedScaleFactor = DATA_MULTIPL; // default x1



            if(cli.hasHelp()) {
                cli.printUsage();
                System.exit(0);
            }

            // spark setup
            SparkConf conf = new SparkConf().setAppName("UniProject");
            JavaSparkContext sc = new JavaSparkContext(conf);
            SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);



            // if contains scale factor
            if(cli.hasScaleFactor()){
                multipliedScaleFactor *= cli.getScaleFactorValue();

            }

            // if contains optional path
            if(cli.hasPath()) {
                filePath = cli.getFilePathValue();
            }


            // load tables
            tables.add(SparkTable.loadLineitemTable(sqlContext, filePath, cli));
            tables.add(SparkTable.loadOrdersTable(sqlContext, filePath, cli));


            if(cli.modeIsRangeOrders()){ // range orders
                cacheTableIfSet(cli, tables);
                runOrdersRanges(sc, sqlContext, multipliedScaleFactor);

            }
            else if(cli.modeIsRangeLineitem()) { // range lineitem
                cacheTableIfSet(cli, tables);
                runLineitemRanges(sc, sqlContext, multipliedScaleFactor);

            }
            else if (cli.modeIsSingleRangeLineitem() && cli.hasRange()) {

                cacheTableIfSet(cli, tables);
                runSingleRangeLineitem(sc, sqlContext, cli.getRangeValue(), multipliedScaleFactor);
            }
            else if (cli.modeIsSingleJoinRangeLineitem() && cli.hasRange()){
                cacheTableIfSet(cli,tables);
                runSingleJoinRangeLineitem(sc, sqlContext, cli.getRangeValue(), multipliedScaleFactor);
            }
            else if (cli.modeIsJoinRangeOrders()) { // join range orders
                cacheTableIfSet(cli, tables);
                runJOINOrdersRanges(sc, sqlContext, multipliedScaleFactor);
            }
            else if (cli.modeIsJoinRangeLineitem()) { // join range lineitem
                cacheTableIfSet(cli, tables);
                runJOINLineitemRanges(sc, sqlContext, multipliedScaleFactor);

            }
            else if (cli.modeIsSaveAsParq()) {

                for (SparkTable table : tables) {
                    table.saveAsParquet();
                }
            }
            else if (cli.modeIsDatabaseTest()) { // single count query to both tables

                runDatabaseTest(sc, sqlContext, multipliedScaleFactor);
            }
            else {
                System.out.println("ERROR: Unrecognized mode!");
                cli.printUsage();
            }


            // close spark
            sc.stop();


        } catch (MissingArgumentException e){System.out.println(e.getMessage());
        } catch (MissingOptionException e) {System.out.println(e.getMessage());
        } catch (ParseException e) {System.out.println(e.getMessage());
        }


    }


	/**
	 * Simple single query to both tables used for testing of new functionality table
	 */
    private static void runDatabaseTest(JavaSparkContext sc, SQLContext sqlContext, int multipliedScaleFactor) {


        sc.setJobGroup("TH", "DB test Orders");
        DataFrame ordersResult = sqlContext.sql("SELECT * FROM orders");
        ordersResult.count();

        sc.setJobGroup("TH", "DB test Orders");
        DataFrame lineitemResult = sqlContext.sql("SELECT * FROM lineitem");
        lineitemResult.count();


    }





	/**
	 * Run series of Lineitem-Orders Joins with range constraint on Orders table
	 */
    private static void runJOINOrdersRanges(JavaSparkContext sc, SQLContext sqlContext, int multipliedScaleFactor) {





        for(int i = 0; i< DATA_RANGE_FACTORS.length; i++) { // shift range value

            for(int j = 0; j<REPEAT_QUERY_NUMBER; j++) { // repeat queries

                sc.setJobGroup("TH", "Order-LineItem JOIN, Order range - (" + (i + 1) + "0%)");
                DataFrame result = sqlContext.sql("SELECT * FROM lineitem  L JOIN orders O ON L.orderkey = O.orderkey WHERE O.orderkey < " + DATA_RANGE_FACTORS[i] * multipliedScaleFactor);
                result.count();

            }
        }

        for(int j = 0; j<REPEAT_QUERY_NUMBER; j++) {
            sc.setJobGroup("TH", "Order-LineItem JOIN, Order range - 100%");
            DataFrame result = sqlContext.sql("SELECT * FROM lineitem  L JOIN orders O ON L.orderkey = O.orderkey");
            result.count();
        }




    }


	/**
	 * Run series of Lineitem-Orders Joins with range constraint on Lineitem table
	 */
    private static void runJOINLineitemRanges(JavaSparkContext sc, SQLContext sqlContext, int multipliedScaleFactor) {

        for(int i = 0; i< DATA_RANGE_FACTORS.length; i++) {

            for(int j = 0; j<REPEAT_QUERY_NUMBER; j++) {

                sc.setJobGroup("TH", "Order-LineItem JOIN, LineItem range - (" + (i + 1) + "0%)");
                DataFrame result = sqlContext.sql("SELECT * FROM lineitem  L JOIN orders O ON L.orderkey = O.orderkey WHERE L.orderkey < " + multipliedScaleFactor);
                result.count();
            }
        }

        for(int j = 0; j<REPEAT_QUERY_NUMBER; j++) {
            sc.setJobGroup("TH", "Order-LineItem JOIN, LineItem range - 100%");
            DataFrame result = sqlContext.sql("SELECT * FROM lineitem  L JOIN orders O ON L.orderkey = O.orderkey");
            result.count();
        }
    }


    // this will only be called on values below 100
    private static int getDataRangeFactor(String rangePercent){


        int firstDigit = Integer.parseInt(rangePercent.substring(0,1));

        return DATA_RANGE_FACTORS[firstDigit - 1];


    }


	/**
	 * Run a single constraint query on Lineitem table
	 */
    private static void runSingleRangeLineitem(JavaSparkContext sc, SQLContext sqlContext, String rangeValue, int multipliedScaleFactor) {

        if(rangeValue.equals("100")){ // 100%

            sc.setJobGroup("TH", "Single Range Lineitem - 100%)");
            DataFrame result = sqlContext.sql("SELECT * FROM lineitem");
            result.count();

        }
        else{

            int dataRangeFactor = getDataRangeFactor(rangeValue);

            sc.setJobGroup("TH", "Single Range Lineitem - " + rangeValue + "%");
            DataFrame result = sqlContext.sql("SELECT * FROM lineitem WHERE orderkey < " + dataRangeFactor * multipliedScaleFactor);
            result.count();
        }
    }

	/**
	 * Run a single Lineitem-Orders Join with range constraint on Lineitem table
	 */
    private static void runSingleJoinRangeLineitem(JavaSparkContext sc, SQLContext sqlContext, String rangeValue, int multipliedScaleFactor) {

        if(rangeValue.equals("100")){

            sc.setJobGroup("TH", "Single Join Range Lineitem - 100%)");
            DataFrame result = sqlContext.sql("SELECT * FROM lineitem  L JOIN orders O ON L.orderkey = O.orderkey");
            result.count();
        }
        else{


            int dataRangeFactor = getDataRangeFactor(rangeValue);

            sc.setJobGroup("TH", "Single Join Range Lineitem - " + rangeValue + "%");
            DataFrame result = sqlContext.sql("SELECT * FROM lineitem  L JOIN orders O ON L.orderkey = O.orderkey WHERE L.orderkey < " + dataRangeFactor * multipliedScaleFactor);
            result.count();


        }


    }

	/**
	 * Run series of constraint queries on Lineitem table
	 */
    private static void runLineitemRanges(JavaSparkContext sc, SQLContext sqlContext, int multipliedScaleFactor) {


        long[] countResults = new long[10];

        for(int i = 0; i< DATA_RANGE_FACTORS.length; i++){
            for(int j = 0; j < REPEAT_QUERY_NUMBER; j++) {

                sc.setJobGroup("TH", "LineItem - (" + (i + 1) + "0%)");
                DataFrame result = sqlContext.sql("SELECT * FROM lineitem WHERE orderkey < " + DATA_RANGE_FACTORS[i] * multipliedScaleFactor);
                countResults[i] = result.count();
            }
        }

        for(int j = 0; j < REPEAT_QUERY_NUMBER; j++) {

            sc.setJobGroup("TH", "LineItem - 100%)");
            DataFrame result = sqlContext.sql("SELECT * FROM lineitem");
            countResults[9] = result.count();
        }


        for(long value : countResults){
            System.out.print(value);
        }
    }


	/**
	 * Run series of constraint queries on Orders table
	 */
    private static void runOrdersRanges(JavaSparkContext sc, SQLContext sqlContext, int multipliedScaleFactor) {
        // Orders 10 to 100% range
        for(int i = 0; i< DATA_RANGE_FACTORS.length; i++){


            for(int j = 0; j < REPEAT_QUERY_NUMBER; j++) {

                sc.setJobGroup("TH", "Orders - (" + (i + 1) + "0%)");
                DataFrame result = sqlContext.sql("SELECT * FROM orders WHERE orderkey < " + DATA_RANGE_FACTORS[i] * multipliedScaleFactor);
                result.count();
            }
        }

        for(int j = 0; j < REPEAT_QUERY_NUMBER; j++) {

            sc.setJobGroup("TH", "Orders - 100%");
            DataFrame result = sqlContext.sql("SELECT * FROM orders");
            result.count();

        }

    }

    private static void cacheTableIfSet(CLI cli, Iterable<SparkTable> tables) {

        if(cli.hasCache()){
            for(SparkTable table : tables){
                table.cache();

            }
        }
    }



}
