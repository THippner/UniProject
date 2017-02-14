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
 * Created by khorm on 18/11/16.
 */


public class Main {

    final static int REPEAT_QUERY_NUMBER = 10;

    final static int DATA_MULTIPL = 100000;
    final static int[] dataRangeFactors = {6, 12, 18, 24, 30, 36, 42, 48, 54};

    final static int DEFAULT_SCALE = 1;
    final static String DEFAULT_PATH = "/user/tomasz/db1/";




    public static void main(String[] args) {


        try {

            CLI cli = new CLI(args);
            List<SparkTable> tables = new ArrayList<SparkTable>();
            int scaleFactor = DEFAULT_SCALE;
            String filePath = DEFAULT_PATH;

            // spark setup
            SparkConf conf = new SparkConf().setAppName("UniProject");
            JavaSparkContext sc = new JavaSparkContext(conf);

            SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);


            if(cli.hasHelp()) {
                cli.printUsage();
                sc.stop();
                System.exit(0);
            }

            // if contains scale factor
            if(cli.hasScaleFactor()){
                scaleFactor = cli.getScaleFactorValue();
            }

            // if contains optional path
            if(cli.hasPath()) {
                filePath = cli.getFilePathValue();
            }


            if(cli.modeIsRangeOrders()){ // range orders

                tables.add(SparkTable.createOrdersTable(sqlContext, filePath));
                cacheTableIfSet(cli, tables);
                runOrdersRanges(sc, sqlContext, scaleFactor);

            }
            else if(cli.modeIsRangeLineitem()) { // range lineitem

                tables.add(SparkTable.createLineitemTable(sqlContext, filePath));
                cacheTableIfSet(cli, tables);
                runLineitemRanges(sc, sqlContext, scaleFactor);

            }
            else if(cli.modeIsJoinRangeOrders()) { // join range orders

                tables.add(SparkTable.createLineitemTable(sqlContext, filePath));
                tables.add(SparkTable.createOrdersTable(sqlContext, filePath));
                cacheTableIfSet(cli, tables);
                runJOINOrdersRanges(sc, sqlContext, scaleFactor);

            }
            else if(cli.modeIsJoinRangeLineitem()) { // join range lineitem

                tables.add(SparkTable.createLineitemTable(sqlContext, filePath));
                tables.add(SparkTable.createOrdersTable(sqlContext, filePath));
                cacheTableIfSet(cli, tables);
                runJOINLineitemRanges(sc, sqlContext, scaleFactor);

            }
            else if (cli.modeIsSaveAsParq()){
                // TODO: df.saveAsParquetFile("file:///home/khorm/TestGrounds/DB/lineitem.parquet");
                System.out.println("NYI (Save as Parq)");
            }
            else{
                System.out.println("ERROR: Unrecognized mode!");
                cli.printUsage();                ;
            }

            // close spark
            sc.stop();


        } catch (MissingArgumentException e){System.out.println(e.getMessage());
        } catch (MissingOptionException e) {System.out.println(e.getMessage());
        } catch (ParseException e) {System.out.println(e.getMessage());
        }


    }

    private static void cacheTableIfSet(CLI cli, Iterable<SparkTable> tables) {

        if(cli.hasCache()){
            for(SparkTable table : tables){
                table.cache();

            }
        }
    }


    // save file
        //result.write().json("file:///home/khorm/TestGrounds/spark-output/out.json");




    private static void runJOINOrdersRanges(JavaSparkContext sc, SQLContext sqlContext, int scaleFactor) {

        for(int i = 0; i< dataRangeFactors.length; i++) { // shift range value

            for(int j = 0; j<REPEAT_QUERY_NUMBER; j++) { // repeat queries

                sc.setJobGroup("TH", "Order-LineItem JOIN, Order range - (" + (i + 1) + "0%)");
                DataFrame result = sqlContext.sql("SELECT * FROM lineitem  L JOIN orders O ON L.orderkey = O.orderkey WHERE O.orderkey < " + dataRangeFactors[i]*DATA_MULTIPL*scaleFactor);
                result.count();

            }
        }

        for(int j = 0; j<REPEAT_QUERY_NUMBER; j++) {
            sc.setJobGroup("TH", "Order-LineItem JOIN, LineItem range - 100%");
            DataFrame result = sqlContext.sql("SELECT * FROM lineitem  L JOIN orders O ON L.orderkey = O.orderkey");
            result.count();
        }
    }



    private static void runJOINLineitemRanges(JavaSparkContext sc, SQLContext sqlContext, int scaleFactor) {

        for(int i = 0; i< dataRangeFactors.length; i++) {

            for(int j = 0; j<REPEAT_QUERY_NUMBER; j++) {

                sc.setJobGroup("TH", "Order-LineItem JOIN, LineItem range - (" + (i + 1) + "0%)");
                DataFrame result = sqlContext.sql("SELECT * FROM lineitem  L JOIN orders O ON L.orderkey = O.orderkey WHERE L.orderkey < " + dataRangeFactors[i]*DATA_MULTIPL*scaleFactor);
                result.count();
            }
        }

        for(int j = 0; j<REPEAT_QUERY_NUMBER; j++) {
            sc.setJobGroup("TH", "Order-LineItem JOIN, LineItem range - 100%");
            DataFrame result = sqlContext.sql("SELECT * FROM lineitem  L JOIN orders O ON L.orderkey = O.orderkey");
            result.count();
        }
    }



    private static void runLineitemRanges(JavaSparkContext sc, SQLContext sqlContext, int scaleFactor) {

        for(int i = 0; i< dataRangeFactors.length; i++){
            for(int j = 0; j < REPEAT_QUERY_NUMBER; j++) {

                sc.setJobGroup("TH", "LineItem - (" + (i + 1) + "0%)");
                DataFrame result = sqlContext.sql("SELECT * FROM lineitem WHERE orderkey < " + dataRangeFactors[i] * DATA_MULTIPL * scaleFactor);
                long z = result.count();
            }
        }

        for(int j = 0; j < REPEAT_QUERY_NUMBER; j++) {

            sc.setJobGroup("TH", "LineItem - 100%)");
            DataFrame result = sqlContext.sql("SELECT * FROM lineitem");
            result.count();
        }

    }



    private static void runOrdersRanges(JavaSparkContext sc, SQLContext sqlContext, int scaleFactor) {
        // Orders 10 to 100% range
        for(int i = 0; i< dataRangeFactors.length; i++){


            for(int j = 0; j < REPEAT_QUERY_NUMBER; j++) {

                sc.setJobGroup("TH", "Orders - (" + (i + 1) + "0%)");
                DataFrame result = sqlContext.sql("SELECT * FROM orders WHERE orderkey < " + dataRangeFactors[i] * DATA_MULTIPL * scaleFactor);
                result.count();
            }
        }

        for(int j = 0; j < REPEAT_QUERY_NUMBER; j++) {

            sc.setJobGroup("TH", "Orders - 100%");
            DataFrame result = sqlContext.sql("SELECT * FROM orders");
            result.count();

        }

    }





}
