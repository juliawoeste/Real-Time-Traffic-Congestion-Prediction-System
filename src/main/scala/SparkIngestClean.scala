//User Story 4 - Spark reads the Kafka messages and streams a dataframe
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object SparkIngestClean {
  def main(args: Array[String]): Unit = {

    // Load config file

    val config = ConfigFactory.parseFile(
      new java.io.File("src/main/resources/application.conf")
    )

    //Read Kafka settings from config
    val bootstrapServers = config.getString("kafka.bootstrapServers")
    val topic = config.getString("kafka.topic")

    //Create Spark session
    println("Starting Spark Session...")

    val spark = SparkSession.builder()
      .appName("KafkaToSparkIngest")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    //Read streaming data from Kafka

    println("Reading in Stream...")

    val kafkaDB = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", topic)
      .option("startingOffsets", "latest")
      .load()

    //Kafka key/value come in as bytes, so convert them to strings
    val jsonDf = kafkaDB.selectExpr(
      "CAST(key AS STRING) as message_key",
      "CAST(value AS STRING) as json_str",
      "topic",
      "partition",
      "offset",
      "timestamp"
    )

    //Define the schema of the JSON being read from Kafka
    val trafficSchema = StructType(Seq(
      StructField("point", StringType, true),
      StructField("currentSpeed", DoubleType, true),
      StructField("freeFlowSpeed", DoubleType, true),
      StructField("currentTravelTime", IntegerType, true),
      StructField("freeFlowTravelTime", IntegerType, true),
      StructField("confidence", DoubleType, true),
      StructField("roadClosure", BooleanType, true),
      StructField("frc", StringType, true)
    ))

    //Parse the JSON string into structured columns
    val trafficDf = jsonDf
      .select(
        col("message_key"),
        from_json(col("json_str"), trafficSchema).alias("data"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp")
      )
      .select(
        col("message_key"),
        col("data.*"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp")
      )


    println("Cleaning Data...")

    val cleanDf = trafficDf
      //Trim Spaces
    .withColumn("point", trim(col("point"))
      )
      .withColumn("frc", upper(trim(col("frc")))
      )

    //Address null values
   .na.fill(Map(
  "point" -> "UNKNOWN",
  "frc" -> "UNKNOWN"
  ))
.na.fill(0.0, Seq("currentSpeed", "freeFlowSpeed", "confidence"))
.na.fill(0, Seq("currentTravelTime", "freeFlowTravelTime"))
.na.fill(false, Seq("roadClosure"))

    //Filter noise/invalid records

    .filter(col("currentSpeed") >= 0)
    .filter(col("freeFlowSpeed") > 0)
    .filter(col("confidence").between(0.0, 1.0)
    )

    //Drop Dupes
    .dropDuplicates("message_key", "offset", "point")


    //Additional Calcs/Classifications
    .withColumn(
    "congestion_ratio",
    col("currentSpeed") / col("freeFlowSpeed")
    )
    .withColumn(
    "delay_seconds",
    col("currentTravelTime") - col("freeFlowTravelTime")
    )
    .withColumn(
    "traffic_condition",
    when(col("roadClosure") === true, "CLOSED")
            //Higher Congestion Ratio = Longer Commutes | 50% ratio = 50% longer that usual
      .when(col("congestion_ratio").between(0.20, 0.49), "MODERATE") //
      .when(col("congestion_ratio").between(0.50, 0.80), "HEAVY")
      .when(col("congestion_ratio").between(0.81, 1.00), "STAND STILL")
      .otherwise("FREE_FLOW")
    )

    //Cleaning Timestamp
    .withColumn("processed_at", current_timestamp())

    //Write the streaming DataFrame to the console
    val query = cleanDf.writeStream
  .format("console")
  .outputMode("append")
  .option("truncate", false)
  .option("checkpointLocation", "checkpoints/traffic_ingest_clean") 
  .start()

    println("Processing Complete.")
    query.awaitTermination()
  }
}
