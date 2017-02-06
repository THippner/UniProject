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



    public static void main(String[] args) {


        final String USAGE = "NYI";

        String file_loc = "/home/tomasz"; // default hdfs directory

        int[] dataRanges = {600000,
                1200000,
                1800000,
                2400000,
                3000000,
                3600000,
                4200000,
                4800000,
                5400000,
                6001215};


        //override default dir if passed as argument
        if(args.length == 0){
            // TODO: print USAGE
            System.out.println("NYI");
        }
        else if(args.length > 1){
            file_loc = args[1];
        }



        // spark setup
        SparkConf conf = new SparkConf().setAppName("UniProject"); //.setMaster("local[2]"); //@TODO  yarn or yarn-client
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);







        if(args[0].equals("1")){
            // 1 range LI with join

            //DataFrame lineitem = importLineItemTable(sqlContext,file_loc);
            //DataFrame orders = importOrdersTable(sqlContext,file_loc);
            //lineitem.registerTempTable("lineitem");
            //orders.registerTempTable("orders");
            // force tables into cache
            //lineitem.cache();
            //lineitem.count();
            //orders.cache();
            //orders.count();

            //runOrdersJOINLineItemRangesLineItem(sc, sqlContext, dataRanges);
            System.out.println("NYI");

        }
        else if (args[0].equals("2")){
            // 2 range O with join

            //DataFrame lineitem = importLineItemTable(sqlContext,file_loc);
            //DataFrame orders = importOrdersTable(sqlContext,file_loc);
            //lineitem.registerTempTable("lineitem");
            //orders.registerTempTable("orders");
            // force tables into cache
            //lineitem.cache();
            //lineitem.count();
            //orders.cache();
            //orders.count();

            //runOrdersJOINLineItemRangesOrders(sc, sqlContext, dataRanges);
            System.out.println("NYI");

        }
        else if (args[0].equals("3")){
            // 3 range LI

            DataFrame lineitem = importLineItemTable(sqlContext,file_loc);
            lineitem.registerTempTable("lineitem");
            lineitem.cache();
            lineitem.count();


            runLineItemRanges(sc, sqlContext, dataRanges);
            //System.out.println("NYI");

        }
        else if (args[0].equals("4")){
            // 4 range O

            //DataFrame orders = importOrdersTable(sqlContext,file_loc);
            //orders.registerTempTable("orders");
            //orders.cache();
            //orders.count();

            //runOrdersRanges(sc, sqlContext, dataRanges);
            System.out.println("NYI");

        }
        else{
            // TODO: print USAGE
            System.out.println("NYI");
        }




        // save file
        //result.write().json("file:///home/khorm/TestGrounds/spark-output/out.json");

        // close Spark
        sc.stop();


    }

    private static void runOrdersJOINLineItemRangesOrders(JavaSparkContext sc, SQLContext sqlContext, int[] dataRanges) {

        for(int i = 0; i< 10; i++) {

            for(int j = 0; j<5; j++) { // repeat each range 5 times

                sc.setJobGroup("TH", "Order-LineItem JOIN, Order range - " + dataRanges[i] + " (" + (i + 1) + "0%)");
                DataFrame result = sqlContext.sql("SELECT * FROM lineitem  L JOIN orders O ON L.orderkey = O.orderkey WHERE O.orderkey < " + dataRanges[i]);
                result.count();

            }
        }


    }

    private static void runOrdersJOINLineItemRangesLineItem(JavaSparkContext sc, SQLContext sqlContext, int[] dataRanges) {

        for(int i = 0; i< 10; i++) {

            for(int j = 0; j<5; j++) { // repeat each range 5 times

                sc.setJobGroup("TH", "Order-LineItem JOIN, LineItem range - " + dataRanges[i] + " (" + (i + 1) + "0%)");
                DataFrame result = sqlContext.sql("SELECT * FROM lineitem  L JOIN orders O ON L.orderkey = O.orderkey WHERE L.orderkey < " + dataRanges[i]);
                result.count();
            }
        }


    }

    private static void runLineItemRanges(JavaSparkContext sc, SQLContext sqlContext, int[] dataRanges) {

        for(int i = 0; i< 10; i++){

            sc.setJobGroup("TH", "LineItem - " + dataRanges[i] + " (" + (i+1) + "0%)");
            DataFrame result = sqlContext.sql("SELECT * FROM lineitem WHERE orderkey < " + dataRanges[i]);
            result.count();
        }

    }

    private static void runOrdersRanges(JavaSparkContext sc, SQLContext sqlContext, int[] dataRanges) {
        // Orders 10 to 100% range
        for(int i = 0; i< 10; i++){

            sc.setJobGroup("TH", "Orders - " + dataRanges[i] + " (" + (i+1) + "0%)");
            DataFrame result = sqlContext.sql("SELECT * FROM orders WHERE orderkey < " + dataRanges[i]);
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
                .load(file_loc +  "/lineitem.tbl"); // @TODO  change


        //df.saveAsParquetFile("file:///home/khorm/TestGrounds/DB/lineitem.parquet");

       return df;
    }

    private static DataFrame importOrdersTable(SQLContext sqlContext){

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
                .load("file:///home/khorm/TestGrounds/DB/orders.tbl"); // @TODO  change


        //df.saveAsParquetFile("file:///home/khorm/TestGrounds/DB/orders.parquet");
        return df;

    }

}
