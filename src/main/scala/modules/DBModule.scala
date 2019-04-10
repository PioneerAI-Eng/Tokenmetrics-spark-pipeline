package modules

import akka.actor.ActorSystem
import com.google.inject.Provides
import com.typesafe.config.Config
import javax.inject.Singleton
import net.codingwell.scalaguice.ScalaModule
import repositories.ReactiveMongo

import scala.concurrent.ExecutionContext

class DBModule extends ScalaModule {

  @Provides
  @Singleton
  def reactiveMongoApi(config: Config): ReactiveMongo = {

    implicit val as: ActorSystem = ActorSystem("mongodb-ActorSystem")
    implicit val ec: ExecutionContext = as.dispatchers.lookup("mongodb-dispatcher")

    val uri = config.getString("mongodb.uri")
    val name = config.getString("mongodb.name")
    new ReactiveMongo(uri, name)
  }
}
