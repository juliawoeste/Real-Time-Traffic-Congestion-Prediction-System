//Prediction Pipline - User Story 5b

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.PipelineModel

object TrafficPrediction {
  def main(args: Array[String]): Unit = {
  
  println("Starting Session...")

    val spark = SparkSession.builder()
      .appName("TrafficPrediction")
      .master("local[*]")
      .getOrCreate()

	println("Loading Data...")
    val model = PipelineModel.load("models/traffic_gbt")
	
	
    val featuresDf = spark.readStream
      .format("parquet")
      .load("data/traffic_features")
	
	
    val predictions = model.transform(featuresDf)
	
	//Running Prediction
	println("Predicting...")
    val query = predictions.select(
      "point",
      "event_time",
      "prediction"
    ).writeStream
      .format("console")
      .outputMode("append")
      .start()

	println("Completed.")
	
    query.awaitTermination()
	
  }
}