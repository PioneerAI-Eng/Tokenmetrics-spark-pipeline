package utils

import org.apache.spark.sql.SparkSession

object SparkUtils {

  val sparkKafka = SparkSession.builder.master("local").appName("kafkaStream").getOrCreate()

  def getKafkaStream() = {

    import sparkKafka.implicits._

    val df = sparkKafka
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test-kafka-topic")
      .load()

    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)]

    df.writeStream.format("console")
  }

}
