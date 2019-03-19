package modules

import com.google.inject.Provides
import com.mongodb.MongoClientSettings
import com.mongodb.reactivestreams.client.MongoDatabase
import com.mongodb.connection.ClusterSettings
import com.mongodb.reactivestreams.client.{MongoClient, MongoClients}
import com.mongodb.{Block, ServerAddress}
import com.typesafe.config.Config
import javax.inject.Singleton
import net.codingwell.scalaguice.ScalaModule

import scala.collection.JavaConverters._

class DBModule extends ScalaModule {

  @Provides
  @Singleton
  def mongoDatabase(mongoClient: MongoClient): MongoDatabase = {
    mongoClient.getDatabase("default")
  }

  @Provides
  @Singleton
  def mongoClient(config: Config): MongoClient = {

    val host = config.getString("mongodb.host")
    val port = config.getNumber("mongodb.port").intValue

    val clusterSettings: ClusterSettings = ClusterSettings.builder().hosts {
      List(new ServerAddress(host, port)).asJava
    }.build
    val block = new Block[ClusterSettings.Builder] {
      override def apply(t: ClusterSettings.Builder): Unit = {
        t.applySettings(clusterSettings)
      }
    }
    val settings = MongoClientSettings
      .builder
      .applyToClusterSettings(block)
      .build
    MongoClients.create(settings)
  }
}
