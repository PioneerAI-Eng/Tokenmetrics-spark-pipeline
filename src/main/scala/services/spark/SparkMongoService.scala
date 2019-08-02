package services.spark

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import org.apache.spark.sql.{DataFrame, SparkSession}

class SparkMongoService {

  /**
    * Connects to MongoDB using Spark connector and loads data from the database.
    *
    * @return a tuple which contains instances of DataFrame and SparkSession
    */
  def loadData(sparkSession: SparkSession): DataFrame = {
   //todo: move configuration into application.conf
    val readConfig = ReadConfig(Map("uri" -> "mongodb://localhost:27017/default.repositories"))

    MongoSpark.load(sparkSession, readConfig)
  }
}
