import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kudu.client.CreateTableOptions
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import collection.JavaConverters._

object SparKuduIntegration {
  def main(args: Array[String]): Unit = {
    // SPARK CONFIGURATION
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark Kudu")
    //val sparkContext = new SparkContext(sparkConf)
    val sparkSession = SparkSession.builder().master("local").appName("Spark Kafka").getOrCreate()

    // SPARK-KAFKA CONFIGURATION
    val df = sparkSession.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe","yahoo")
        .load()
    val df_value = df.selectExpr("CAST(value AS STRING)")
    val query = df_value.writeStream
      .outputMode("append")
      .format("console")
      .start()
      .awaitTermination()


//    // CREATE KUDU TABLE
//    val master1 = "localhost:7051"
//    val master2 = "localhost:7151"
//    val master3 = "localhost:7251"
//    val kuduMasters = Seq(master1,master2,master3).mkString(",")
//
//    val kuduContext = new KuduContext(kuduMasters, sparkContext)
//
//    val kuduTableName = "student"
//
//    val kuduStudentSchema = StructType(Array(
//      StructField("id", IntegerType, false),
//      StructField("name", StringType, true),
//      StructField("age", IntegerType, true)
//    ))
//    val kuduStudentPrimaryKey = Seq("id")
//
//    if (kuduContext.tableExists(kuduTableName))
//      kuduContext.deleteTable(kuduTableName)
//    else
//      kuduContext.createTable(
//        kuduTableName,
//        kuduStudentSchema,
//        kuduStudentPrimaryKey,
//        new CreateTableOptions().setNumReplicas(1).addHashPartitions(List("id").asJava,3)
//      )


//    sparkStreamingContext.start()
//    sparkStreamingContext.awaitTermination()
  }
}
