// this file is to test the TrafficFeatures.scala in the terminal
import org.apache.spark.sql.SparkSession

object InspectTrafficFeatures {
  def main(args: Array[String]): Unit = {

    println("Starting inspection...")

    val spark = SparkSession.builder()
      .appName("InspectTrafficFeatures")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val df = spark.read.parquet("data/traffic_features")

    println("Schema:")
    df.printSchema()

    println("Sample Data:")
    df.show(false)

    spark.stop()
  }
}