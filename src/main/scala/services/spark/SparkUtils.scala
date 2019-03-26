package services.spark

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import org.apache.spark.sql.SparkSession

object SparkUtils {

  def getDataFrameResult() = {

    val spark = SparkSession.builder()
      .master("local")
      .appName("MongoSparkConnectorIntro")
      .config("spark.mongodb.input.uri", "mongodb://localhost:27017/default")
      .config("spark.mongodb.input.readPreference.name", "secondaryPreferred")
      .config("spark.mongodb.output.uri", "mongodb://localhost:27017/default")
      .getOrCreate()

    //for read from multiple collection in the future
    val readConfig = ReadConfig(Map("uri" -> "mongodb://localhost:27017/default.repositories"))

    (MongoSpark.load(spark,readConfig), spark)
  }
}
