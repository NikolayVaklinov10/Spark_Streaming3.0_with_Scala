package part2structuredstreaming

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, length}
import common._
import org.apache.spark.sql.streaming.Trigger

import scala.concurrent.duration.DurationInt

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

    // transformation
    val shortLines: DataFrame = lines.filter(length(col("value")) <= 5)

    // tell between a static vs a streaming DF
    println(shortLines.isStreaming)

    val query = shortLines.writeStream
      .format("console")
      .outputMode("append")
      .start()

    query.awaitTermination()
  }

  def readFromFiles() = {
    val stocksDF: DataFrame = spark.readStream
      .format("csv")
      .option("header", "false")
      .option("dateFormat", "MMM d yyyy")
      .schema(stocksSchema)
      .load("src/main/resources/data/stocks")

    stocksDF.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  def demoTriggers() = {
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    // write the lines DF at a certain trigger
    lines.writeStream
      .format("console")
      .outputMode("append")
      .trigger(
        //Trigger.ProcessingTime(2.seconds) // every 2 seconds run the query
        //Trigger.Once() // single batch, then terminate
        Trigger.Continuous(2.seconds) // experimental, every 2 seconds create a batch with whatever you have
      )
      .start()
      .awaitTermination()

  }

  def main(args: Array[String]): Unit = {
    demoTriggers()
  }

}
