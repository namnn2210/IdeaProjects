import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.split
import org.apache.spark.sql.functions.col
import collection.JavaConverters._

object KafkaToSparkToKudu {
  def main(args: Array[String]): Unit = {
    // SPARK CONFIGURATION
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark Kudu")
    val sparkContext = new SparkContext(sparkConf)
    val sparkSession = SparkSession.builder().master("local").appName("Spark Kafka").getOrCreate()

    // SPARK-KAFKA CONFIGURATION
    val df = sparkSession.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "yahoo")
      .load()

    val yahooSchema = StructType(Array(
      StructField("fullExchangeName", StringType, false),
      StructField("exchangeTimezoneName", StringType),
      StructField("symbol", StringType),
      StructField("regularMarketChangeRaw", StringType),
      StructField("regularMarketChangeFmt", StringType),
      StructField("gmtOffSetMilliseconds", StringType),
      StructField("headSymbolAsString", StringType),
      StructField("exchangeDataDelayedBy", StringType),
      StructField("language", StringType),
      StructField("regularMarketTimeRaw", StringType),
      StructField("regularMarketTimeFmt", StringType),
      StructField("exchangeTimezoneShortName", StringType),
      StructField("regularMarketChangePercentRaw", StringType),
      StructField("regularMarketChangePercentFmt", StringType),
      StructField("marketState", StringType),
      StructField("regularMarketPriceRaw", StringType),
      StructField("regularMarketPriceFmt", StringType),
      StructField("quoteType", StringType),
      StructField("tradeable", StringType),
      StructField("market", StringType),
      StructField("sourceInterval", StringType),
      StructField("exchange", StringType),
      StructField("region", StringType),
      StructField("triggerable", StringType),
      StructField("shortName", StringType),
      StructField("regularMarketPreviousCloseRaw", StringType),
      StructField("regularMarketPreviousCloseFmt", StringType),
      StructField("priceHint", StringType),
      StructField("currency", StringType),
      StructField("longName", StringType),
      StructField("quoteSourceName", StringType)
    ))

    val df_value = df.withColumn("temp", split(col("value"), "\\,"))
      .select(
        (0 until 30).map(i => col("temp").getItem(i).as(yahooSchema.fieldNames.apply(i))): _*)

    val master1 = "localhost:7051"
    val master2 = "localhost:7151"
    val master3 = "localhost:7251"
    val kuduMasters = Seq(master1, master2, master3).mkString(",")

    val kuduContext = new KuduContext(kuduMasters, sparkContext)


    val kuduTableName = "yahoo"

    //SET KUDU TABLE PRIMARY KEY
    val kuduYahooPrimaryKey = Seq("fullExchangeName")

    //DROP KUDU TABLE IF EXISTS
    if (kuduContext.tableExists(kuduTableName))
      kuduContext.deleteTable(kuduTableName)

    //CREATE KUDU TABLE
    kuduContext.createTable(
      kuduTableName,
      yahooSchema,
      kuduYahooPrimaryKey,
      new CreateTableOptions().setNumReplicas(1).addHashPartitions(List("fullExchangeName").asJava, 3)
    )

    //WRITE STREAM TO KUDU TABLE
    df_value.writeStream
      .options(Map("kudu.master" -> "localhost:7051", "kudu.table" -> "yahoo"))
      .option("checkpointLocation", "location")
      .outputMode("append")
      .format("kudu")
      .start()
      .awaitTermination()
  }
}
