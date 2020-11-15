package part3lowlevel

import java.io.{File, FileWriter}
import java.sql.Date
import java.text.SimpleDateFormat

import common.Stock
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DStreams {

  val spark  = SparkSession.builder()
    .appName("DStreams")
    .master("local[2]")
    .getOrCreate()

  /*
  Spark Streaming Context = entry point to the DStreams API
  - needs the spark context
  - a duration = batch interval
   */

  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  /*
  - define input sources by creating DStreams
  - define transformations on DStreams
  - call an actions on DStreams
  - start ALL computations with ssc.start()
  - no more computations can be added
  - await termination, or stop the computation
  - you cannot restart the ssc
   */

  def readFromSocket() = {
    val socketStream: DStream[String] = ssc.socketTextStream("localhost", 12345)

    // transformation = lazy
    val wordsStream: DStream[String] = socketStream.flatMap(line => line.split(" "))

    // action
    wordsStream.print()

    ssc.start()
    ssc.awaitTermination()
  }

  def createNewFile() = {
    new Thread(() => {
      Thread.sleep(5000)

      val path = "src/main/resources/data/stocks"
      val dir = new File(path)
      val nFiles = dir.listFiles().length
      val newFile = new File(s"$path/newStocks$nFiles.csv")
      newFile.createNewFile()

      val writer = new FileWriter(newFile)
      writer.write(
        """
          |AAPL,Jan 1 2000,25.94
          |AAPL,Feb 1 2000,28.66
          |AAPL,Mar 1 2000,33.95
          |AAPL,Apr 1 2000,31.01
          |AAPL,May 1 2000,21
          |AAPL,Jun 1 2000,26.19
          |AAPL,Jul 1 2000,25.41
          |AAPL,Aug 1 2000,30.47
          |""".stripMargin.trim)

      writer.close()

    }).start()
  }

  def readFromFile(): Unit = {
    createNewFile() // operates on another thread
    // defined DStream
    val stocksFilePath = "src/main/resources/data/stocks"
    val textStream: DStream[String] = ssc.textFileStream(stocksFilePath)

    // transformations
    val dataFormat = new SimpleDateFormat("MMM d yyyy")

    val stocksStream:DStream[Stock] = textStream.map{ line =>
      val tokens = line.split(",")
      val company = tokens(0)
      val date = new Date(dataFormat.parse(tokens(1)).getTime)
      val price = tokens(2).toDouble

      Stock(company, date, price)
    }

    // action
    stocksStream.print()

    // start the computations
    ssc.start()
    ssc.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    readFromSocket()
  }

}
