name := "technology-graph"

version := "0.1"

scalaVersion := "2.12.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.0",
  "org.apache.spark" %% "spark-sql" % "2.4.0",
  "org.apache.spark" % "spark-sql-kafka-0-10_2.11" % "2.4.0",
  "org.apache.kafka" %% "kafka" % "2.1.0",
  "org.apache.kafka" % "kafka-clients" % "2.1.1",

  "com.typesafe.akka" %% "akka-http" % "10.1.7",
  "com.typesafe.akka" %% "akka-stream" % "2.5.21",
  "io.spray" %% "spray-json" % "1.3.5",

  "com.google.inject" % "guice" % "4.1.0",
  "net.codingwell" %% "scala-guice" % "4.2.1",

  "com.lightbend.akka" %% "akka-stream-alpakka-mongodb" % "1.0-M3",
  "org.mongodb" % "mongodb-driver-reactivestreams" % "1.11.0",
  "org.mongodb.scala" %% "mongo-scala-driver" % "2.6.0",

  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.typesafe.akka" %% "akka-slf4j" % "2.5.16"
)