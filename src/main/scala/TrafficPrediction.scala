//Prediction Pipline - User Story 5b

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

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
      StructField("traffic_events_10min", LongType, false),
      StructField("road_closure_flag", IntegerType, true)
    ))

    val featuresDf = spark.readStream
      .schema(featuresSchema)
      .format("parquet")
      .load("data/traffic_features")
	
	
    val predictions = model.transform(featuresDf)
      .withColumnRenamed("prediction", "predicted_delay_seconds")
      .withColumn(
        "traffic_condition",
        when(col("road_closure_flag") === 1, "CLOSED")
          .when(col("avg_congestion_10min") < 0.5, "STAND_STILL")
          .when(col("avg_congestion_10min") < 0.75, "HEAVY")
          .when(col("avg_congestion_10min") < 0.98, "MODERATE")
          .otherwise("FREE_FLOW")
      )
	
	//Running Prediction
	println("Predicting...")

 val query = predictions.select(
      col("point").alias("location"),
      col("window_start"),
      col("window_end"),
      round(col("avg_congestion_10min"), 2).alias("avg_congestion_10min"),
      col("traffic_condition"),
      round(col("avg_delay_10min"), 2).alias("actual_avg_delay_seconds"),
      round(col("predicted_delay_seconds"), 2).alias("predicted_delay_seconds")
    ).writeStream
      .format("console")
      .outputMode("append")
      .option("truncate", false)
      .start()

	println("Completed.")
	
  query.awaitTermination()
	
  }
}