import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.SparkSession

object KuduController {

  case class Yahoo(
                    fullExchangeName: String,
                    exchangeTimezoneName: String,
                    symbol: String,
                    regularMarketChangeRaw: String,
                    regularMarketChangeFmt: String,
                    gmtOffSetMilliseconds: String,
                    headSymbolAsString: String,
                    exchangeDataDelayedBy: String,
                    language: String,
                    regularMarketTimeRaw: String,
                    regularMarketTimeFmt: String,
                    exchangeTimezoneShortName: String,
                    regularMarketChangePercentRaw: String,
                    regularMarketChangePercentFmt: String,
                    marketState: String,
                    regularMarketPriceRaw: String,
                    regularMarketPriceFmt: String,
                    quoteType: String,
                    tradeable: String,
                    market: String,
                    sourceInterval: String,
                    exchange: String,
                    region: String,
                    triggerable: String,
                    shortName: String,
                    regularMarketPreviousCloseRaw: String,
                    regularMarketPreviousCloseFmt: String,
                    priceHint: String,
                    currency: String,
                    longName: String,
                    quoteSourceName: String
                  )

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local").appName("Kudu").getOrCreate()

    val master1 = "localhost:7051"
    val master2 = "localhost:7151"
    val master3 = "localhost:7251"
    val kuduMasters = Seq(master1, master2, master3).mkString(",")

    val kuduContext = new KuduContext(kuduMasters, sparkSession.sparkContext)

    val df = sparkSession.read.options(Map("kudu.master" -> "localhost:7051",
      "kudu.table" -> "yahoo")).format("kudu").load
    df.createOrReplaceTempView("my_table")
    println("==========BEFORE UPDATE==========")
    sparkSession.sql("select * from my_table").show()

    val update_df = Array(Yahoo("CME",
      "namngocngo22",
      "RTY=F",
      "5.5",
      "5.50",
      "14400000",
      "RTY=F",
      "10",
      "vn",
      "1587622899",
      "2:21AM",
      "EDT",
      "0.46028957",
      "0.46", "nam", "nam", "nam", "nam", "nam", "nam", "nam", "nam", "nam", "nam", "nam", "nam", "nam", "nam", "nam", "nam", "nam"))

    import sparkSession.implicits._

    kuduContext.updateRows(sparkSession.sparkContext.parallelize(update_df).toDF(), "yahoo")

    println("==========AFTER UPDATE==========")
    sparkSession.sql("select * from my_table").show()
  }
}