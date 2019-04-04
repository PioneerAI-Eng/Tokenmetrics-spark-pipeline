package modules

import com.google.inject.Provides
import com.typesafe.config.Config
import javax.inject.Singleton
import net.codingwell.scalaguice.ScalaModule
import repositories.ReactiveMongo

import scala.concurrent.ExecutionContext

class DBModule extends ScalaModule {

  @Provides
  @Singleton
//  todo: add custom execution context
  def reactiveMongoApi(config: Config)(implicit ec: ExecutionContext): ReactiveMongo = {
    val uri = config.getString("mongodb.uri")
    val name = config.getString("mongodb.name")
    new ReactiveMongo(uri, name)
  }
}
