package part2structuredstreaming

import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.functions._
import common._

object StreamingDatasets {

  val spark = SparkSession.builder()
    .appName("Streaming Datasets")
    .master("local[2]")
    .getOrCreate()

  // the implicit needed for the conversion
  import spark.implicits._

  def readCars() = {
    // useful for DF -> DS transformations
    val carEncoder = Encoders.product[Car]

    spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load() // DF with single string column "value"
      .select(from_json(col("value"), carsSchema).as("car")) // composite column (struct)
      .selectExpr("car.*") // DF with multiple columns
      .as[Car](carEncoder) // encoder can be passed implicitly with spark.implicits

  }




  def main(args: Array[String]): Unit = {

  }

}
