package part4integrations

import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object IntegratingKafkaDStreams {

  val spark = SparkSession.builder()
    .appName("Spark DStreams + Kafka")
    .master("local[2]")
    .getOrCreate()

  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  val kafkaParams: Map[String, Object] = Map(
    "bootstrap.servers" -> "localhost:9092",
    "key.serializer" -> classOf[StringSerializer], // send data to kafka
    "value.serializer" -> classOf[StringSerializer],
    "key.deserializer" -> classOf[StringDeserializer], // receiving data from kafka
    "value.deserializer" -> classOf[StringDeserializer],
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> false.asInstanceOf[Object]
  )



  def main(args: Array[String]): Unit = {

  }

}
