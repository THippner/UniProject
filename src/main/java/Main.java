import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.ParseException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Created by khorm on 18/11/16.
 */


public class Main {

    final static int REPEAT_QUERY_NUMBER = 10;

    final static int DATA_MULTIPL = 100000;
    final static int[] dataRangeFactors = {6, 12, 18, 24, 30, 36, 42, 48, 54};

    final static int DEFAULT_SCALE = 1;
    final static String DEFAULT_PATH = "/user/tomasz/db1/";
    final static String LINEITEM = "lineitem";
    final static String ORDERS = "orders";



    public static void main(String[] args) {


        try {

            CLI cli = new CLI(args);
            int scaleFactor = DEFAULT_SCALE;
            String filePath = DEFAULT_PATH;

            // spark setup
            SparkConf conf = new SparkConf().setAppName("UniProject");
            JavaSparkContext sc = new JavaSparkContext(conf);
            //sc.addJar("/home/tomasz/UniProject.jar");
            SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);


            if(cli.hasHelp()) {
                cli.printUsage();
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

                DataFrame lineitem = importLineItemTable(sqlContext,filePath);
                DataFrame orders = importOrdersTable(sqlContext,filePath);
                lineitem.registerTempTable(LINEITEM);
                orders.registerTempTable(ORDERS);

                // force tables into cache
                if(cli.hasCache()){
                    lineitem.cache();
                    lineitem.count();
                    orders.cache();
                    orders.count();
                }
                runOrdersRanges(sc, sqlContext, scaleFactor);
            }
            else if(cli.modeIsRangeLineitem()) { // range lineitem
                //TODO:
            }
            else if(cli.modeIsJoinRangeOrders()) { // join range orders
                //TODO:
            }
            else if(cli.modeIsJoinRangeLineitem()) { // join range lineitem
                //TODO:
            }
            else if (cli.modeIsSaveAsParq()){
                // TODO: df.saveAsParquetFile("file:///home/khorm/TestGrounds/DB/lineitem.parquet");
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








//
//
//        if(args[0].equals("1")){
//            // 1 range on LineItem with join
//
//            DataFrame lineitem = importLineItemTable(sqlContext,file_loc);
//            DataFrame orders = importOrdersTable(sqlContext,file_loc);
//            lineitem.registerTempTable("lineitem");
//            orders.registerTempTable("orders");
//
//            // force table into cache
//            lineitem.cache();
//            lineitem.count();
//            orders.cache();
//            orders.count();
//
//            runOrdersJOINLineItemRangesLineItem(sc, sqlContext);
//
//
//        }
//        else if (args[0].equals("2")){
//            // 2 range on Order with join
//
//            DataFrame lineitem = importLineItemTable(sqlContext,file_loc);
//            DataFrame orders = importOrdersTable(sqlContext,file_loc);
//            lineitem.registerTempTable("lineitem");
//            orders.registerTempTable("orders");
//
//            // force tables into cache
//            lineitem.cache();
//            lineitem.count();
//            orders.cache();
//            orders.count();
//
//            runOrdersJOINLineItemRangesOrders(sc, sqlContext);
//
//
//        }
//        else if (args[0].equals("3")){
//            // 3 range on LineItem
//
//            DataFrame lineitem = importLineItemTable(sqlContext,file_loc);
//            lineitem.registerTempTable("lineitem");
//
//            // force table into cache
//            lineitem.cache();
//            lineitem.count();
//
//            runLineItemRanges(sc, sqlContext);
//
//        }
//        else if (args[0].equals("4")){
//            // 4 range on Order
//
//            DataFrame orders = importOrdersTable(sqlContext,file_loc);
//            orders.registerTempTable("orders");
//
//            // force table into cache
//            orders.cache();
//            orders.count();
//
//            runOrdersRanges(sc, sqlContext);
//
//        }
//



        // save file
        //result.write().json("file:///home/khorm/TestGrounds/spark-output/out.json");



        // close Spark
        //sc.stop();



    private static void runOrdersJOINLineItemRangesOrders(JavaSparkContext sc, SQLContext sqlContext, int scaleFactor) {

        for(int i = 0; i< dataRangeFactors.length; i++) { // shift range value

            for(int j = 0; j<REPEAT_QUERY_NUMBER; j++) { // repeat queries

                sc.setJobGroup("TH", "Order-LineItem JOIN, Order range - " + dataRangeFactors[i] + " (" + (i + 1) + "0%)");
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



    private static void runOrdersJOINLineItemRangesLineItem(JavaSparkContext sc, SQLContext sqlContext, int scaleFactor) {

        for(int i = 0; i< dataRangeFactors.length; i++) {

            for(int j = 0; j<REPEAT_QUERY_NUMBER; j++) {

                sc.setJobGroup("TH", "Order-LineItem JOIN, LineItem range - " + dataRangeFactors[i] + " (" + (i + 1) + "0%)");
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



    private static void runLineItemRanges(JavaSparkContext sc, SQLContext sqlContext, int scaleFactor) {

        for(int i = 0; i< dataRangeFactors.length; i++){

            sc.setJobGroup("TH", "LineItem - " + dataRangeFactors[i] + " (" + (i+1) + "0%)");
            DataFrame result = sqlContext.sql("SELECT * FROM lineitem WHERE orderkey < " + dataRangeFactors[i]*DATA_MULTIPL*scaleFactor);
            result.count();
        }

    }



    private static void runOrdersRanges(JavaSparkContext sc, SQLContext sqlContext, int scaleFactor) {
        // Orders 10 to 100% range
        for(int i = 0; i< dataRangeFactors.length; i++){

            sc.setJobGroup("TH", "Orders - " + dataRangeFactors[i] + " (" + (i+1) + "0%)");
            DataFrame result = sqlContext.sql("SELECT * FROM orders WHERE orderkey < " + dataRangeFactors[i]*DATA_MULTIPL*scaleFactor);
            result.count();
        }
    }



    private static DataFrame importLineItemTable(SQLContext sqlContext, String file_loc) {

        //example
        //1|63700|3701|3|8|13309.60|0.10|0.02|N|O|1996-01-29|1996-03-05|1996-01-31|TAKE BACK RETURN|REG AIR|riously. regular, express dep|
        //1009858|55486|5487|5|47|67749.56|0.02|0.04|A|F|1993-10-28|1993-10-30|1993-11-15|COLLECT COD|FOB|ubt slyly ironic a|
        //1009858|149067|6610|6|42|46874.52|0.04|0.08|A|F|1993-08-19|1993-10-25|1993-08-26|NONE|FOB|s can use. bold, regular instr|


        // LINEITEM Table Layout
        // Column Name Datatype Requirements Comment
        // L_ORDERKEY identifier Foreign Key to O_ORDERKEY
        // L_PARTKEY identifier Foreign key to P_PARTKEY, first part of the compound Foreign Key to (PS_PARTKEY, PS_SUPPKEY) with L_SUPPKEY
        // L_SUPPKEY Identifier Foreign key to S_SUPPKEY, second part of the compound Foreign Key to (PS_PARTKEY,
        // L_LINENUMBER integer
        // L_QUANTITY decimal
        // L_EXTENDEDPRICE decimal
        // L_DISCOUNT decimal
        // L_TAX decimal
        // L_RETURNFLAG fixed text, size 1
        // L_LINESTATUS fixed text, size 1
        // L_SHIPDATE date
        // L_COMMITDATE date
        // L_RECEIPTDATE date
        // L_SHIPINSTRUCT fixed text, size 25
        // L_SHIPMODE fixed text, size 10
        // L_COMMENT variable text size 44
        // Primary Key: L_ORDERKEY, L_LINENUMBER

        StructType customSchema = new StructType(new StructField[] {
                new StructField("orderkey", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("partkey", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("suppkey", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("linenumber", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("quantity", DataTypes.IntegerType, true, Metadata.empty()), // original schema was decimal
                new StructField("extendedprice", DataTypes.createDecimalType(10,2), true, Metadata.empty()),
                new StructField("discount", DataTypes.createDecimalType(3,2), true, Metadata.empty()),
                new StructField("tax", DataTypes.createDecimalType(3,2), true, Metadata.empty()),
                new StructField("returnflag", DataTypes.StringType, true, Metadata.empty()),
                new StructField("linestatus", DataTypes.StringType, true, Metadata.empty()),
                new StructField("shipdate", DataTypes.DateType, true, Metadata.empty()),
                new StructField("commitdate", DataTypes.DateType, true, Metadata.empty()),
                new StructField("receiptdate", DataTypes.DateType, true, Metadata.empty()),
                new StructField("shipinstruct", DataTypes.StringType, true, Metadata.empty()),
                new StructField("shipmode", DataTypes.StringType, true, Metadata.empty()),
                new StructField("comment", DataTypes.StringType, true, Metadata.empty())
        });


        DataFrame df = sqlContext.read()
                .format("com.databricks.spark.csv")
                .schema(customSchema)
                .option("delimiter", "|")
                .option("dateFormat", "YYYY-MM-DD")
                .load(file_loc +  "lineitem.tbl");

       return df;
    }



    private static DataFrame importOrdersTable(SQLContext sqlContext, String file_loc){

        //example
        // 1|36901|O|173665.47|1996-01-02|5-LOW|Clerk#000000951|0|nstructions sleep furiously among |



        // ORDERS Table Layout
        // Column Name Datatype Requirements Comment
        // O_ORDERKEY Identifier SF*1,500,000 are sparsely populated
        // O_CUSTKEY Identifier Foreign Key to C_CUSTKEY
        // O_ORDERSTATUS fixed text, size 1
        // O_TOTALPRICE Decimal
        // O_ORDERDATE Date
        // O_ORDERPRIORITY fixed text, size 15
        // O_CLERK fixed text, size 15
        // O_SHIPPRIORITY Integer
        // O_COMMENT variable text, size 79
        // Primary Key: O_ORDERKEY


        StructType customSchema = new StructType(new StructField[] {
                new StructField("orderkey", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("custkey", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("orderstatus", DataTypes.StringType, true, Metadata.empty()),
                new StructField("totalprice", DataTypes.createDecimalType(10,2), true, Metadata.empty()),
                new StructField("orderdate", DataTypes.DateType, true, Metadata.empty()),
                new StructField("orderpriority", DataTypes.StringType, true, Metadata.empty()),
                new StructField("clerk", DataTypes.StringType, true, Metadata.empty()),
                new StructField("shippriority", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("comment", DataTypes.StringType, true, Metadata.empty())
        });



        DataFrame df = sqlContext.read()
                .format("com.databricks.spark.csv")
                .schema(customSchema)
                .option("delimiter", "|")
                .option("dateFormat", "YYYY-MM-DD")
                .load(file_loc +  "orders.tbl");

        return df;

    }

}
