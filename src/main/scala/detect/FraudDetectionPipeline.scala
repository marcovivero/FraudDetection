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

    // Get variable names.
    data.columns.foreach(println)

    // See first data format.
    data.show(1)

  }


}