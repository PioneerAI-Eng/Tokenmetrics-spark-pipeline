package services.kafka

import java.time.Duration
import java.util
import java.util.{Collections, Properties}

import com.google.inject.Inject
import com.typesafe.config.Config
import org.apache.kafka.clients.admin._
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

//todo: make methods asynchronous
class KafkaService @Inject()(config: Config) {

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
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("kafka-services.bootstrap-servers-config"))
    props.put(ProducerConfig.CLIENT_ID_CONFIG, config.getString("kafka-services.producer-data.client-id-config"))
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, config.getString("kafka-services.key-serializer-class-config"))
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, config.getString("kafka-services.value-serializer-class-config"))
    props.put(ProducerConfig.RETRIES_CONFIG, config.getInt("kafka-services.producer-data.retries-config").asInstanceOf[Integer])

    new KafkaProducer[String, String](props)
  }

  def createConsumer(topicName: String): KafkaConsumer[String, String] = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("kafka-services.bootstrap-servers-config"))
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, config.getString("kafka-services.producer-data.client-id-config"))
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, config.getString("kafka-services.value-serializer-class-config"))
    props.put(ConsumerConfig.GROUP_ID_CONFIG, config.getString("kafka-services.consumer-data.group-id-config"))
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, config.getString("kafka-services.consumer-data.enable-auto-commit-config"))
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, config.getString("kafka-services.consumer-data.auto-commit-interval-ms-config"))

    new KafkaConsumer[String, String](props)
  }

  def createTopic(topicName: String, numPartitions: Int, replicationFactor: Short): CreateTopicsResult = {
    val props = new Properties()
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("kafka-services.bootstrap-servers-config"))
    val adminClient = AdminClient.create(props)
    val createTopicResult = adminClient.createTopics(util.Arrays.asList(new NewTopic(topicName, numPartitions, replicationFactor)))
    adminClient.close()

    createTopicResult
  }

  def removeTopic(topicName: String): DeleteTopicsResult = {
    val props = new Properties()
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("kafka-services.bootstrap-servers-config"))
    val adminClient = AdminClient.create(props)
    val deleteTopicResult = adminClient.deleteTopics(util.Arrays.asList(topicName))
    adminClient.close()
    deleteTopicResult
  }

  def topics: util.Map[String, TopicListing] = {
    val props = new Properties
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("kafka-services.bootstrap-servers-config"))
    val adminClient = AdminClient.create(props)
    adminClient.listTopics.namesToListings.get
  }
}
