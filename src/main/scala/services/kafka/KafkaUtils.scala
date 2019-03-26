package services.kafka

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
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.RETRIES_CONFIG, new Integer(0))

    new KafkaProducer[String, String](props)
  }

  def createKafkaConsumer(topicName: String): KafkaConsumer[String, String] = {

    val props = new Properties()

    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "something")
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")

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

  def removeTopicIntoKafka(topicName: String): DeleteTopicsResult = {

    val props = new Properties()

    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")

    val adminClient = AdminClient.create(props)

    val result = adminClient.deleteTopics(util.Arrays.asList(topicName))
    adminClient.close()

    result
  }

  def getTopicList: util.Map[String, TopicListing] = {
    val props = new Properties()
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    val adminClient = AdminClient.create(props)
    adminClient.listTopics().namesToListings().get()
  }

}
