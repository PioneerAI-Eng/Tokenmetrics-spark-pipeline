package services.spark

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

class SparkMongoService {

  /**
    * Connects to MongoDB using Spark connector and loads data from the database.
    *
    * @return a tuple which contains instances of DataFrame and SparkSession
    */
  def loadData: (DataFrame, SparkSession) = {

    //todo: move configuration into application.conf
    val configs = Map(
      "spark.mongodb.input.uri" -> "mongodb://localhost:27017/default",
      "spark.mongodb.input.readPreference.name" -> "secondaryPreferred",
      "spark.mongodb.output.uri" -> "mongodb://localhost:27017/default")

    val sparkConfig = configs.foldRight(new SparkConf())((values, sparkConf) => sparkConf.set(values._1, values._2))
    //todo: create an instance SparkSession through 'Guice'
    //todo: configure multiple SparkSession if it need
    val sparkSession = SparkSession.builder()
      .master("local") //todo: change configuration when using Spark on a cluster
      .appName("MongoSparkConnector")
      .config(sparkConfig)
      .getOrCreate
    //todo: move configuration into application.conf
    val readConfig = ReadConfig(Map("uri" -> "mongodb://localhost:27017/default.repositories"))

    (MongoSpark.load(sparkSession, readConfig), sparkSession)
  }
}
