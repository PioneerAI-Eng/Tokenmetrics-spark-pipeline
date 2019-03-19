package models

import spray.json.{DefaultJsonProtocol, RootJsonFormat}

case class Dependency(packageName: String, requirements: String)

object DependencyProtocol extends DefaultJsonProtocol{
  implicit val dependencyFormat: RootJsonFormat[Dependency] = jsonFormat2(Dependency)
}