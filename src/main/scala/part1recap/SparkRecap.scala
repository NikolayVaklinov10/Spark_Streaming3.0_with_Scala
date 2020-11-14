package part1recap

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, expr}

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
  val usefulCarsData = cars.select(
    col("Name"), // column object
    $"Year" // another column object
      (col("Weight_in_lbs") / 2.2).as("Weight_in_kg"),
    expr("Weight_in_lbs / 2.2").as("Weight_in_kg2")
  )

  val carsWeights = cars.selectExpr("Weight_in_lbs / 2.2")

  // filter
  //             or        where()     which is identical
  val europeanCars = cars.filter(col("Origin") =!= "USA")


    // aggregations
  val averageHP = cars.select(avg(col("Horsepower")).as("average_hp")) // sum mean, stddev, min max

  // grouping
  val countByOrigin = cars
    .groupBy(col("Origin")) // a relationalGroupedDataset
    .count()

  // joining
  val guitarPlayers = spark.read
    .option("inferSchema", "true")
    .json("src/main/resource/data/guitarPlayers")

  val bands = spark.read
    .option("inferSchema", "true")
    .json("src/main/resource/data/bands")

  //                                                                             OR   ===
  val guitaristsBands = guitarPlayers.join(bands, guitarPlayers.col("band") equalTo  bands.col("id"))

  // datasets = typed distributed collection of objects
  case class GuitarPlayer(id: Long, name: String, guitars: Seq[Long], band: Long)
  val guitarPlayersDS = guitarPlayers.as[GuitarPlayer] // needs spark.implicits
  guitarPlayersDS.map(_.name)

  // Spark SQL
  cars.createOrReplaceTempView("cars")
  val americanCars = spark.sql(
    """
      |select Name from cars where Origin = 'USA'
      |""".stripMargin
  )

  // low-level API: RDDs
  val sc = spark.sparkContext
  val numbersRDD:RDD[Int] = sc.parallelize(1 to 1000000)

  val doubles = numbersRDD.map(_ * 2)

  // RDD -> DF
  val numbersDF = numbersRDD.toDF("number") // you lose type info, but you gain the sql capabilities

  // RDD -> DS
  val numbersDS = spark.createDataset(numbersRDD)

  // DS -> RDD
  val guitarPlayerRDD = guitarPlayersDS.rdd

  // DF -> RDD
  val carsRDD = cars.rdd // RDD[Row]





}
