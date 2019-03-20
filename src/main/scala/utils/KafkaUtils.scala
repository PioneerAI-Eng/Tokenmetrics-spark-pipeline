package utils

import java.util
import java.util.{Collections, Properties}

import kafka.utils.ZkUtils
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
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "something")
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")

    val consumer = new KafkaConsumer[String, String](props)

    consumer.subscribe(Collections.singletonList(topicName))
    consumer
  }

  def createTopicIntoKafka(topicName: String, numPartitions: Int, replicationFactor: Short): CreateTopicsResult = {

    val props = new Properties()

    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")

    val adminClient = AdminClient.create(props)

    val result = adminClient.createTopics(util.Arrays.asList(new NewTopic(topicName, numPartitions, replicationFactor)))
    adminClient.close()

    result
  }

  def getTopicList(): util.Map[String, TopicListing] = {
    val props = new Properties()
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    val adminClient = AdminClient.create(props)
    adminClient.listTopics().namesToListings().get()
  }

}
