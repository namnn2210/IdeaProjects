import org.apache.spark.sql.SparkSession

object KuduController {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local").appName("Spark Kafka").getOrCreate()
    val df = sparkSession.read.options(Map("kudu.master" -> "192.168.1.100:7051",
      "kudu.table" -> "yahoo")).format("kudu").load
    df.createOrReplaceTempView("my_table")
    sparkSession.sql("select * from my_table").show()
  }
}
