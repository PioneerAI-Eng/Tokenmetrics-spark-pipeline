package modules

import com.typesafe.config.{Config, ConfigFactory}
import net.codingwell.scalaguice.ScalaModule

class ConfigModule extends ScalaModule{
  override def configure(): Unit = {
    bind[Config].toInstance(ConfigFactory.load())
  }
}
