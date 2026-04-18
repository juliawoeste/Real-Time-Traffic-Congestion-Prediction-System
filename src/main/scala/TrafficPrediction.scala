//Prediction Pipline - User Story 5b

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.types._

object TrafficPrediction {
  def main(args: Array[String]): Unit = {
  
  println("Starting Session...")

    val spark = SparkSession.builder()
      .appName("TrafficPrediction")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

	println("Loading Data...")
    val model = PipelineModel.load("models/traffic_gbt")
	

  val featuresSchema = StructType(Seq(
      StructField("point", StringType, false),
      StructField("window_start", TimestampType, true),
      StructField("window_end", TimestampType, true),
      StructField("avg_speed_10min", DoubleType, true),
      StructField("avg_delay_10min", DoubleType, true),
      StructField("avg_congestion_10min", DoubleType, true),
      StructField("max_speed_10min", DoubleType, true),
      StructField("min_speed_10min", DoubleType, true),
      StructField("traffic_events_10min", LongType, false)
    ))

    val featuresDf = spark.readStream
      .schema(featuresSchema)
      .format("parquet")
      .load("data/traffic_features")
	
	
    val predictions = model.transform(featuresDf)
	
	//Running Prediction
	println("Predicting...")
    val query = predictions.select(
      "point",
      "window_start",
      "window_end",
      "avg_delay_10min",
      "prediction"
    ).writeStream
      .format("console")
      .outputMode("append")
      .option("truncate", false)
      .start()

	println("Completed.")
	
    query.awaitTermination()
	
  }
}