//User Story 6- GraphX 
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object RoadGraphAnalysis {
  def main(args: Array[String]): Unit = {

    println("Starting Road Graph Analysis...")

    val config = ConfigFactory.parseFile(
      new java.io.File("src/main/resources/application.conf")
    )

    val bootstrapServers = config.getString("kafka.bootstrapServers")
    val topic = config.getString("kafka.graphTopic")

    val spark = SparkSession.builder()
      .appName("RoadGraphAnalysis")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    // Read graph data topic as a batch
    val kafkaDf = spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .load()

    // Schema for coordinates
    val coordinateSchema = StructType(Seq(
      StructField("latitude", DoubleType, true),
      StructField("longitude", DoubleType, true)
    ))

    // Schema for graph JSON message
    val trafficSchema = StructType(Seq(
      StructField("point", StringType, true),
      StructField("currentSpeed", DoubleType, true),
      StructField("freeFlowSpeed", DoubleType, true),
      StructField("currentTravelTime", IntegerType, true),
      StructField("freeFlowTravelTime", IntegerType, true),
      StructField("confidence", DoubleType, true),
      StructField("roadClosure", BooleanType, true),
      StructField("frc", StringType, true),
      StructField("coordinates", ArrayType(coordinateSchema), true)
    ))

    // Parse Kafka JSON into columns
    val parsedDf = kafkaDf
      .selectExpr("CAST(value AS STRING) as json_str")
      .select(from_json(col("json_str"), trafficSchema).alias("data"))
      .select("data.*")
      .filter(col("coordinates").isNotNull)
      .filter(size(col("coordinates")) > 1)

    //removes duplicates
    val uniqueParsedDf = parsedDf.dropDuplicates("point", "currentSpeed", "freeFlowSpeed", "currentTravelTime", "freeFlowTravelTime")

    //limits number of rows for testing
    val limitedDf = uniqueParsedDf.limit(5)

    println("Parsed graph-ready traffic data:")
    limitedDf
        .select(
            col("point"),
            col("currentSpeed"),
            col("freeFlowSpeed"),
            size(col("coordinates")).alias("num_coordinates")
    )
        .show(5, false)
    
    
    // Seperates coordinates array into seperate rows, gives postion of array and coordinate
    val explodedDf = limitedDf
        .select(
        col("point"),
        col("currentSpeed"),
        col("freeFlowSpeed"),
        col("currentTravelTime"),
        col("freeFlowTravelTime"),
        posexplode(col("coordinates")).as(Seq("idx", "coord"))
    )
        .select(
        col("point"),
        col("currentSpeed"),
        col("freeFlowSpeed"),
        col("currentTravelTime"),
        col("freeFlowTravelTime"),
        col("idx"),
        col("coord.latitude").alias("latitude"),
        col("coord.longitude").alias("longitude")
  )
    // Create vertices - each unique coordinate becomes a vertex
    val verticesDf = explodedDf
      .withColumn("vertexKey", concat_ws(",", col("latitude"), col("longitude")))
      .withColumn("vertexId", abs(hash(col("vertexKey"))).cast("long"))
      .select("vertexId", "latitude", "longitude")
      .distinct()

    // Create edges by linking consecutive coordinate points
    val a = explodedDf.alias("a")
    val b = explodedDf.alias("b")

    //join consecutive coordinates with traffic metrics
    val edgesDf = a.join(
        b,
        col("a.point") === col("b.point") && col("a.idx") + 1 === col("b.idx")
    )
    .withColumn("srcKey", concat_ws(",", col("a.latitude"), col("a.longitude")))
    .withColumn("dstKey", concat_ws(",", col("b.latitude"), col("b.longitude")))
    .withColumn("srcId", abs(hash(col("srcKey"))).cast("long"))
    .withColumn("dstId", abs(hash(col("dstKey"))).cast("long"))
    .withColumn(
        "congestionRatio",
        when(col("a.freeFlowSpeed") > 0, col("a.currentSpeed") / col("a.freeFlowSpeed"))
        .otherwise(lit(0.0))
  )
    .withColumn(
        "delaySeconds",
        col("a.currentTravelTime") - col("a.freeFlowTravelTime")
    )
    .select(
        col("srcId"),
        col("dstId"),
        col("a.point").alias("point"),
        col("congestionRatio"),
        col("delaySeconds")
  )

    //removes duplicate edges
    val uniqueEdgesDf = edgesDf.distinct()


    println("Sample Graph edges:")
    uniqueEdgesDf.show(10, false)

    // Convert vertices DataFrame to GraphX vertex RDD
    val vertices: RDD[(VertexId, (Double, Double))] = verticesDf.rdd.map { row =>
      (
        row.getAs[Long]("vertexId"),
        (
          row.getAs[Double]("latitude"),
          row.getAs[Double]("longitude")
        )
      )
    }

    // Convert edges DataFrame to GraphX edge RDD
    val edges: RDD[Edge[(String, Double, Int)]] = uniqueEdgesDf.rdd.map { row =>
      Edge(
        row.getAs[Long]("srcId"),
        row.getAs[Long]("dstId"),
        (
          row.getAs[String]("point"),
          row.getAs[Double]("congestionRatio"),
          row.getAs[Int]("delaySeconds")
        )
      )
    }

    //builds graph with vertices and edges
    val graph = Graph(vertices, edges)

    //verify
    println("\nRoad graph created successfully.")
    println(s"Number of vertices: ${graph.vertices.count()}")
    println(s"Number of edges: ${graph.edges.count()}")

    //ANALYSIS (6b)

    //count how many edges are connected to each node
    //higher degree = more connected = more important road point
   println("\nTop Connected Road Points:")
graph.degrees
  .takeOrdered(10)(Ordering[Int].reverse.on(_._2))
  .foreach { case (nodeId, degree) =>
    println(s"Node $nodeId has $degree road connections")
  }

println("\nBottleneck Segments (Lowest Congestion Ratio):")
graph.edges
  .map(e => (e.attr._1, e.srcId, e.dstId, e.attr._2, e.attr._3))
  .takeOrdered(10)(Ordering[Double].on(_._4))
  .foreach { case (point, srcId, dstId, congestionRatio, delaySeconds) =>
    println(
      f"Location: $point | Segment: $srcId -> $dstId | Congestion Ratio: $congestionRatio%.3f | Delay: $delaySeconds seconds"
    )
  }

println("\nBottleneck Segments (Highest Delay):")
graph.edges
  .map(e => (e.attr._1, e.srcId, e.dstId, e.attr._2, e.attr._3))
  .takeOrdered(10)(Ordering[Int].reverse.on(_._5))
  .foreach { case (point, srcId, dstId, congestionRatio, delaySeconds) =>
    println(
      f"Location: $point | Segment: $srcId -> $dstId | Congestion Ratio: $congestionRatio%.3f | Delay: $delaySeconds seconds"
    )
  }

    spark.stop()
  }
}