package services.spark

import com.google.inject.Inject
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import com.typesafe.config.Config
import scala.collection.JavaConversions._

class SparkContextConf @Inject()(config: Config) {

  val configs = asScalaSet(config.getConfig("spark.default").entrySet())
    .map(entry => entry.getKey -> entry.getValue.unwrapped().toString).toMap

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
