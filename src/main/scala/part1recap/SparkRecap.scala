package part1recap

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object SparkRecap extends App {

  val spark = SparkSession.builder()
    .appName("Spark Recap")
    .master("local[2]")
    .getOrCreate()

  // read a DF
  val cars = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/cars")

  // showing a DF to the console
  cars.printSchema()
  cars.show()

  // widely used in practice
  import spark.implicits._
  // the select function
  cars.select(
    col("Name"), // column object
    $"Year" // another column object
  )

}
