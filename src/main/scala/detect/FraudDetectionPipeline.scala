package detect

import io.prediction.workflow.FakeRun
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext

object FraudDetectionPipeline extends FakeRun {

  func = main

  def main (sc: SparkContext): Unit = {

    // Initialize SQLContext requirements.
    val sql : SQLContext = new SQLContext(sc)
    import sql.implicits._

    // Import data from Parquet file.
    val data : DataFrame = sql.parquetFile("/data/fraud-detection/deduped.small.parquet")

    // Number of observations:
    ws
    println("Number of observations:")
    println(data.count)
    ws

    // Get variable names and data type.
    ws
    println("(Variable Names, Data Type):")
    data.dtypes.foreach(println)
    ws

    // Output:
    // (amount,DoubleType)
    // (binNumber,StringType)
    // (binCountry,StringType)
    // (cardNumber,StringType)
    // (customerEmail,StringType)
    // (customerPhone,StringType)
    // (device,StringType)
    // (ip,StringType)
    // (name,StringType)
    // (riskScore,IntegerType)
    // (timestamp,TimestampType)
    // (rejected,BooleanType)

    // See first data format.
    //// data.show(100)

    // Note: a lot of false rejections, need to inspect rejection = true data.
    //// data.filter(data("rejected") === true).show(10)


    // Select features that are not so user specific, for purposes
    // of generalized classification.
    val selectedData : DataFrame = data.select(
      "rejected",
      "amount",
      "binCountry",
      "device",
      "riskScore"
    )

    // First five data observations (should be false):
    ws
    println("Data:")
    selectedData.show(5)
    ws

    ws
    println("Data (true):")
    selectedData.filter(selectedData("rejected") === true).show(5)
    ws
  }

  def ws {println(" ")}


}