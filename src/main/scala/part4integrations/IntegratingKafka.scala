package part4integrations

import org.apache.spark.sql.SparkSession

object IntegratingKafka {

  val spark = SparkSession.builder()
    .appName("Integrating Kafka")
    .master("local[2]")
    .getOrCreate()




  def main(args: Array[String]): Unit = {

  }
}
