package part2structuredstreaming

import org.apache.spark.sql.SparkSession

object StreamingDatasets {

  val spark = SparkSession.builder()
    .appName("Streaming Datasets")
    .master("local[2]")
    .getOrCreate()




  def main(args: Array[String]): Unit = {

  }

}
