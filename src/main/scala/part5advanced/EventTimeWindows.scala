package part5advanced

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}

object EventTimeWindows {

  val spark = SparkSession.builder()
    .appName("Event Time Windows")
    .master("local[2]")
    .getOrCreate()

  val onlinePurchaseSchema = StructType(Array(
    StructField("id", StringType),
    StructField("time", TimestampType),
    StructField("item", StringType),
    StructField("quantity", IntegerType)
  ))







  def main(args: Array[String]): Unit = {

  }

}
