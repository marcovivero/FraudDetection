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
    val data : DataFrame = sql.parquetFile("/data/fraud-detection/all.parquet")

    // Get variable names and data type.
    data.dtypes.foreach(println)

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
    data.show(100)

    // Note: a lot of false rejections, need to inspect rejection = true data.
    data.filter("rejected === true").show(10)

    val selectedData : DataFrame = data.select(
      "rejected",
      "amount",
      "binCountry",
      "device",
      "riskScore"
    )

    selectedData.show(5)
    selectedData.filter("rejected === true").show(5)
  }


}