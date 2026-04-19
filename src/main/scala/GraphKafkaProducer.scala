//this is a seperate kafka producer due to the lack of coordinates in the other producers - didnt want to mess up other files
import com.typesafe.config.ConfigFactory
import io.circe.Json
import io.circe.syntax._
import io.circe.parser._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import requests._

import java.util.Properties

object TomTomGraphKafkaProducer {
  def main(args: Array[String]): Unit = {

    val config = ConfigFactory.parseFile(
      new java.io.File("src/main/resources/application.conf")
    )

    val apiKey = config.getString("tomtom.apiKey")
    val point = config.getString("tomtom.point")
    val bootstrapServers = config.getString("kafka.bootstrapServers")
    val topic = config.getString("kafka.graphTopic")

    val props = new Properties()
    props.put("bootstrap.servers", bootstrapServers)
    props.put("key.serializer", classOf[StringSerializer].getName)
    props.put("value.serializer", classOf[StringSerializer].getName)

    val producer = new KafkaProducer[String, String](props)

    val url =
      s"https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json" +
        s"?key=$apiKey&point=$point&unit=mph"

    println("Starting graph Kafka producer...")

    try {
      while (true) {
        println("Calling TomTom API for graph data...")

        val response = requests.get(
          url,
          readTimeout = 30000,
          connectTimeout = 10000
        )

        if (response.statusCode == 200) {
          val rawJson = response.text()
          val parsed = parse(rawJson).getOrElse(
            throw new Exception("Failed to parse TomTom JSON")
          )

          val cursor = parsed.hcursor.downField("flowSegmentData")

          val coordinatesJson =
            cursor.downField("coordinates")
              .downField("coordinate")
              .focus
              .getOrElse(Json.arr())

          val kafkaMessage: Json = Json.obj(
            "point" -> point.asJson,
            "currentSpeed" -> cursor.get[Double]("currentSpeed").getOrElse(0.0).asJson,
            "freeFlowSpeed" -> cursor.get[Double]("freeFlowSpeed").getOrElse(0.0).asJson,
            "currentTravelTime" -> cursor.get[Int]("currentTravelTime").getOrElse(0).asJson,
            "freeFlowTravelTime" -> cursor.get[Int]("freeFlowTravelTime").getOrElse(0).asJson,
            "confidence" -> cursor.get[Double]("confidence").getOrElse(0.0).asJson,
            "roadClosure" -> cursor.get[Boolean]("roadClosure").getOrElse(false).asJson,
            "frc" -> cursor.get[String]("frc").getOrElse("UNKNOWN").asJson,
            "coordinates" -> coordinatesJson
          )

          val record = new ProducerRecord[String, String](
            topic,
            point,
            kafkaMessage.noSpaces
          )

          val metadata = producer.send(record).get()
          println(s"Sent graph message to ${metadata.topic()} offset ${metadata.offset()}")
        } else {
          println(s"API Error: ${response.statusCode}")
        }

        Thread.sleep(10000)
      }
    } finally {
      producer.flush()
      producer.close()
    }
  }
}