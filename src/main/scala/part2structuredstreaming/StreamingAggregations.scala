package part2structuredstreaming

import org.apache.spark.sql.SparkSession

object StreamingAggregations {

  val spark = SparkSession.builder()
    .appName("Streaming Aggregations")
    .master("local[2]")
    .getOrCreate()

  


  def main(args: Array[String]): Unit = {

  }

}
