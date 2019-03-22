package services

import java.time.Duration

import org.apache.kafka.clients.producer.ProducerRecord
import utils.KafkaUtils

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

  def readFromKafka(topicName: String) = {

    val consumer = KafkaUtils.createKafkaConsumer(topicName)

    consumer.poll(Duration.ofSeconds(1))
  }

}
