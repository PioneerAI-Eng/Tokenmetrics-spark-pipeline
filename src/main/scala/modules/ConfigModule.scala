package modules

import java.io.File

import com.google.inject.{Provides, Singleton}
import com.typesafe.config.{Config, ConfigFactory}
import net.codingwell.scalaguice.ScalaModule

class ConfigModule extends ScalaModule {

  @Provides
  @Singleton
  def config: Config = {
    val conf = ConfigFactory.parseFile(new File("src/main/resources/application.conf"))
    ConfigFactory.load(conf)
  }
}
