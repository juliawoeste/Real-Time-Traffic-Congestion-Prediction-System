import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger

object TrafficFeaturesClean {
  def main(args: Array[String]): Unit = {

println("Starting Session...")

    val config = ConfigFactory.parseFile(
      new java.io.File("src/main/resources/application.conf")
    )

    val spark = SparkSession.builder()
      .appName("TrafficFeaturesClean")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

  //Ingestion
  println("Ingesting Data...")

    val kafkaDf = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", config.getString("kafka.bootstrapServers"))
      .option("subscribe", config.getString("kafka.topic"))
      .option("startingOffsets", "earliest")
      .load()

    val schema = StructType(Seq(
      StructField("point", StringType),
      StructField("currentSpeed", DoubleType),
      StructField("freeFlowSpeed", DoubleType),
      StructField("currentTravelTime", IntegerType),
      StructField("freeFlowTravelTime", IntegerType),
      StructField("confidence", DoubleType),
      StructField("roadClosure", BooleanType),
      StructField("frc", StringType)
    ))

    val parsedDf = kafkaDf
      .selectExpr("CAST(value AS STRING)", "timestamp")
      .select(
        from_json(col("value"), schema).alias("data"),
        col("timestamp")
      )
      .select("data.*", "timestamp")

    //Cleaning & Features
    println("Cleaning Data & Assigning Features...")

    val baseDf = parsedDf
      .withColumn("event_time", col("timestamp"))
      .withColumn("point", trim(col("point")))
      .withColumn("frc", upper(trim(col("frc"))))

      .na.fill(Map(
  "point" -> "UNKNOWN",
  "frc" -> "UNKNOWN"
    ))
      .na.fill(0.0, Seq("currentSpeed", "freeFlowSpeed", "confidence"))
      .na.fill(0, Seq("currentTravelTime", "freeFlowTravelTime"))
      .na.fill(false, Seq("roadClosure"))
      
      .filter(col("freeFlowSpeed") > 0)
      .filter(col("freeFlowTravelTime") > 0)
      .filter(col("confidence").between(0.0, 1.0))
      .withColumn("congestion_ratio",
        col("currentSpeed") / col("freeFlowSpeed")
      )
      .withColumn("delay_seconds",
        col("currentTravelTime") - col("freeFlowTravelTime")
      )
      .withColumn("traffic_condition",
        when(col("roadClosure"), "CLOSED")
          .when(col("congestion_ratio") < 0.3, "STAND STILL")
          .when(col("congestion_ratio") < 0.6, "HEAVY")
          .when(col("congestion_ratio") < 0.9, "MODERATE")
          .otherwise("FREE_FLOW")
      )

	//Aggregations
  println("Creating Aggregations...")

    val featuresDf = baseDf
      .withWatermark("event_time", "10 minutes")
      .groupBy(
        col("point"),
        window(col("event_time"), "10 minutes")
      )
      .agg(
        avg("currentSpeed").alias("avg_speed_10min"),
        avg("delay_seconds").alias("avg_delay_10min"),
        avg("congestion_ratio").alias("avg_congestion_10min"),
        max("currentSpeed").alias("max_speed_10min"),
        min("currentSpeed").alias("min_speed_10min"),
        count("*").alias("traffic_events_10min")
      )
      .select(
        col("point"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("avg_speed_10min"),
        col("avg_delay_10min"),
        col("avg_congestion_10min"),
        col("max_speed_10min"),
        col("min_speed_10min"),
        col("traffic_events_10min")
      )

  //Write
  println("Writing query to DF...")

    val query = featuresDf.writeStream
      .format("parquet")
      .option("path", "data/traffic_features")
      .option("checkpointLocation", "checkpoints/traffic_features_v1")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("30 seconds"))
      .start()

  //println("Query complete...")
    query.awaitTermination()
  }
}