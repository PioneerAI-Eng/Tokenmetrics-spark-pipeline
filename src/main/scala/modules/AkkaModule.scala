package modules

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import net.codingwell.scalaguice.ScalaModule

import scala.concurrent.ExecutionContext

class AkkaModule extends ScalaModule {

  override def configure(): Unit = {
    implicit val as: ActorSystem = ActorSystem("global-actor-system")
    bind[ActorSystem].toInstance(as)
    bind[ActorMaterializer].toInstance(ActorMaterializer())
    bind[ExecutionContext].toInstance(as.dispatcher)
  }
}