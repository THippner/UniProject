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

        SparkConf conf = new SparkConf().setAppName("test").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);


        convertOrdersTable(sqlContext);
        convertLineItemTable(sqlContext);




        sc.stop();


    }

    private static void convertLineItemTable(SQLContext sqlContext) {

        //example
        //1|63700|3701|3|8|13309.60|0.10|0.02|N|O|1996-01-29|1996-03-05|1996-01-31|TAKE BACK RETURN|REG AIR|riously. regular, express dep|

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
