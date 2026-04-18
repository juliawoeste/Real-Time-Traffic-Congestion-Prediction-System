//Training Data Model - User Story 5b

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.GBTRegressor
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.functions._

object TrainTrafficModel {
  def main(args: Array[String]): Unit = {

	println("Starting Session...")

    val spark = SparkSession.builder()
      .appName("TrainTrafficModel")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    //Query for training data
    println("Create Training DF...")
	
    val df = spark.read.parquet("data/traffic_features").na.drop()

	//Assigning Features
  println("Preparing DataFrame")

  val trainingDf = df.select(
      col("point"),
      col("window_start"),
      col("window_end"),
      col("avg_speed_10min"),
      col("avg_delay_10min"),
      col("avg_congestion_10min"),
      col("max_speed_10min"),
      col("min_speed_10min"),
      col("traffic_events_10min")
    )
  
  println("Creating feature vector")

    val assembler = new VectorAssembler()
      .setInputCols(Array(
        "avg_speed_10min",
        "avg_congestion_10min",
        "max_speed_10min",
        "min_speed_10min",
        "traffic_events_10min"
      ))
      .setOutputCol("features")
		
		//Setting model parameters
      println("Setting Model Parameters...")

    val gbt = new GBTRegressor()
      .setLabelCol("avg_delay_10min")
      .setFeaturesCol("features")
      .setMaxIter(20)
      .setMaxDepth(5)
      .setStepSize(0.1)

    val pipeline = new Pipeline()
      .setStages(Array(assembler, gbt))

	//Run Pipeline, Return Result
	println("Run Pipeline, Return Result")
    val model = pipeline.fit(trainingDf)


	//Save output
	println("Saving...")
    model.write.overwrite().save("models/traffic_gbt")
	
	println("Complete.")
  }
}