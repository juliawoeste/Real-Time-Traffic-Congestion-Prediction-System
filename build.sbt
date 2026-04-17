scalaVersion := "2.12.18"

lazy val root = project
    .in(file("."))
    .settings(
    name := "temp_vs",
    version := "0.1.0-SNAPSHOT",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.7",
      "org.apache.spark" %% "spark-sql" % "3.5.7",
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.7",
      "org.apache.spark" %% "spark-mllib" % "2.2.0",
      "org.apache.spark" %% "spark-mllib" % "3.5.0",
      "com.typesafe" % "config" % "1.4.3",
      "com.lihaoyi" %% "requests" % "0.8.0",
      "io.circe" %% "circe-parser" % "0.14.7",
      "io.circe" %% "circe-core" % "0.14.7",
      "io.circe" %% "circe-generic" % "0.14.7",
      "org.apache.kafka" % "kafka-clients" % "3.5.1",
      "org.scalameta" %% "munit" % "1.0.0" % Test
  ),

  Compile / run / fork := true,
  Compile / run / javaOptions ++= Seq(
      "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
    )
)

   
