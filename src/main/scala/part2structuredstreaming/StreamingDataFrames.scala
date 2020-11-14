package part2structuredstreaming

import org.apache.spark.sql.{ DataFrame, SparkSession }

object StreamingDataFrames {

  // reading a DataFrame
  val spark = SparkSession.builder()
    .appName("First Streaming App")
    .master("local[2]")
    .getOrCreate()

  // consume the DataFrame
  def readFromSocket() = {
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    val query = lines.writeStream
      .format("console")
      .outputMode("append")
      .start()

    query.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    readFromSocket()
  }

}
