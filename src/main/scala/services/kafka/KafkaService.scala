package services.kafka

import java.time.Duration
import java.util
import java.util.{Collections, Properties}

import org.apache.kafka.clients.admin._
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

//todo: make methods asynchronous
class KafkaService {

  def send(topicName: String, key: String, value: String): Unit = {

    if (!topics.containsKey(topicName)) {
      createTopic(topicName, numPartitions = 1, replicationFactor = 1)
    }

    val producer = createProducer
    val record = new ProducerRecord[String, String](topicName, key, value)
    producer.send(record)
    producer.close()
  }

  def consume(topicName: String): ConsumerRecords[String, String] = {
    val consumer = createConsumer(topicName)
    consumer.subscribe(Collections.singletonList(topicName))
    consumer.poll(Duration.ofSeconds(10))
  }

  def createProducer: KafkaProducer[String, String] = {
    //todo: move producer configuration into application.conf
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaProducer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.RETRIES_CONFIG, new Integer(0))

    new KafkaProducer[String, String](props)
  }

  def createConsumer(topicName: String): KafkaConsumer[String, String] = {
    //todo: move consumer configuration into application.conf
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "something")
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")

    new KafkaConsumer[String, String](props)
  }

  def createTopic(topicName: String, numPartitions: Int, replicationFactor: Short): CreateTopicsResult = {
    //todo: move configuration into application.conf
    val props = new Properties()
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    val adminClient = AdminClient.create(props)
    val createTopicResult = adminClient.createTopics(util.Arrays.asList(new NewTopic(topicName, numPartitions, replicationFactor)))
    adminClient.close()

    createTopicResult
  }

  def removeTopic(topicName: String): DeleteTopicsResult = {
    //todo: move configuration into application.conf
    val props = new Properties()
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    val adminClient = AdminClient.create(props)
    val deleteTopicResult = adminClient.deleteTopics(util.Arrays.asList(topicName))
    adminClient.close()
    deleteTopicResult
  }

  def topics: util.Map[String, TopicListing] = {
    //todo: move configuration into application.conf
    val props = new Properties
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    val adminClient = AdminClient.create(props)
    adminClient.listTopics.namesToListings.get
  }
}
