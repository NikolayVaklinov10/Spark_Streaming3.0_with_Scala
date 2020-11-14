package part1recap

import org.apache.spark.sql.SparkSession

object SparkRecap extends App {

  val spark = SparkSession.builder()
    .appName("Spark Recap")
    .master("local[2]")
    .getOrCreate()

  // read a DF
  val cars = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/car")

  cars.printSchema()
  cars.show()

}
