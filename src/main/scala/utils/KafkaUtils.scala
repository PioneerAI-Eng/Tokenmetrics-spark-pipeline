package utils

import java.util
import java.util.{Collections, Properties}

import org.apache.kafka.clients.admin._
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}

object KafkaUtils {

  def createKafkaProducer(): KafkaProducer[String, String] = {

    val props = new Properties()

    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaProducer")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    new KafkaProducer[String, String](props)
  }

  def createConsumer(topicName: String): KafkaConsumer[String, String] = {

    val props = new Properties()

    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "something")

    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(Collections.singletonList(topicName))
    consumer
  }

  def createTopicIntoKafka(topicName: String, numPartitions: Int, replicationFactor: Short): CreateTopicsResult = {

    val props = new Properties()

    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:2181")

    val zkClient = AdminClient.create(props)

    zkClient.createTopics(util.Arrays.asList(new NewTopic(topicName, numPartitions, replicationFactor)))
  }

  def getTopicList(): ListTopicsResult = {

    val props = new Properties()

    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:2181")

    val zkClient = AdminClient.create(props)

    zkClient.listTopics()
  }

}
