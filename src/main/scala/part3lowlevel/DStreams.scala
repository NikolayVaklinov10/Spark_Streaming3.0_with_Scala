package part3lowlevel

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DStreams {

  val spark  = SparkSession.builder()
    .appName("DStreams")
    .master("local[2]")
    .getOrCreate()

  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  def main(args: Array[String]): Unit = {
    
  }

}
