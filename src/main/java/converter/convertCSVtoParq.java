package converter;

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


public class convertCSVtoParq {



    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("test2").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);


        //convertOrdersTable(sqlContext);
        convertLineItemTable(sqlContext);




        sc.stop();


    }

    private static void convertLineItemTable(SQLContext sqlContext) {

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
                .load("file:///home/khorm/TestGrounds/DB/lineitem.tbl");


        //df.saveAsParquetFile("file:///home/khorm/TestGrounds/DB/lineitem.parquet");

        // execute test query
        df.registerTempTable("lineitem");

        DataFrame result = sqlContext.sql("SELECT * FROM lineitem WHERE orderkey < 600");

        result.write().json("file:///home/khorm/TestGrounds/spark-output/out.json");


    }

    private static void convertOrdersTable(SQLContext sqlContext){

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
                .load("file:///home/khorm/TestGrounds/DB/orders.tbl");


        df.saveAsParquetFile("file:///home/khorm/TestGrounds/DB/orders.parquet");

    }

}
