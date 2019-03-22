package services

import java.time.Duration
import java.util

import models.GitHubRepository
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import utils.{KafkaUtils, SparkUtils}

trait KafkaServices {

  def sendToKafka(topicName: String, key: String, value: String): Unit = {

    if (!KafkaUtils.getTopicList().containsKey(topicName)) {
      KafkaUtils.createTopicIntoKafka(topicName, 1, 1)
    }

    val producer = KafkaUtils.createKafkaProducer()
    val record = new ProducerRecord[String, String](topicName, key, value)
    producer.send(record)
    producer.close()
  }

  def readFromKafka(topicName: String): ConsumerRecords[String, String] = {

    val consumer = KafkaUtils.createKafkaConsumer(topicName)

    consumer.poll(Duration.ofSeconds(10))
  }

  def readFromMongoBySpark(): (util.List[GitHubRepository], SparkSession) = {

    implicit val gitHubRepositoryEncoder: Encoder[GitHubRepository] = Encoders.product[GitHubRepository]

    val resultFrame = SparkUtils.getDataFrameResult()

    (resultFrame._1.as[GitHubRepository].collectAsList(), resultFrame._2)
  }

}
