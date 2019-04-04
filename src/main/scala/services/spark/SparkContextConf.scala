package services.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

class SparkContextConf {

  //todo: move configuration into application.conf
  val configs = Map(
    "spark.mongodb.input.uri" -> "mongodb://localhost:27017/default",
    "spark.mongodb.input.readPreference.name" -> "secondaryPreferred",
    "spark.mongodb.output.uri" -> "mongodb://localhost:27017/default",
    "spark.neo4j.bolt.url" -> "bolt://127.0.0.1:7687",
    "spark.neo4j.bolt.user" -> "neo4j",
    "spark.neo4j.bolt.password" -> "niger182")

  val sparkConfig: SparkConf = configs.foldRight(new SparkConf())((values, sparkConf) => sparkConf.set(values._1, values._2))

  def getSparkSession(master: String, appName: String): SparkSession = {
    val sparkSession = SparkSession.builder()
      .master(master)
      .appName(appName)
      .config(sparkConfig)
      .getOrCreate()
      .newSession()

    sparkSession
  }

}
