//Training Data Model - User Story 5b

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.GBTRegressor
import org.apache.spark.ml.Pipeline

object TrainTrafficModel {
  def main(args: Array[String]): Unit = {

	println("Starting Session...")

    val spark = SparkSession.builder()
      .appName("TrainTrafficModel")
      .master("local[*]")
      .getOrCreate()

    //Query for training data
    println("Create Training DF...")
	
    val df = spark.read.parquet("data/traffic_features").na.drop()

	//Assigning Features
  println("Creating feature array")

    val assembler = new VectorAssembler()
      .setInputCols(Array(
        "currentSpeed",
        "freeFlowSpeed",
        "congestion_ratio",
        "hour",
        "frc_index",
        "avg_speed_last10min",
        "avg_delay_last10min"
      ))
      .setOutputCol("features")
		
		//Setting model parameters
      println("Setting Model Parameters...")

    val gbt = new GBTRegressor()
      .setLabelCol("delay_seconds")
      .setFeaturesCol("features")
      .setMaxIter(100)
      .setMaxDepth(5)
      .setStepSize(0.1)

    val pipeline = new Pipeline()
      .setStages(Array(assembler, gbt))

	//Run Pipeline, Return Result
	println("Run Pipeline, Return Result")
    val model = pipeline.fit(df)


	//Save output
	println("Saving...")
    model.write.overwrite().save("models/traffic_gbt")
	
	println("Complete.")
  }
}