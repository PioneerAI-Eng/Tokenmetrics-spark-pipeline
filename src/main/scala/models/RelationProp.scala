package models

import spray.json.{DefaultJsonProtocol, RootJsonFormat}

case class RelationProp(weight: Int)

object RelationPropProtocol extends DefaultJsonProtocol{
  implicit val relationPropFormat: RootJsonFormat[RelationProp] = jsonFormat1(RelationProp)
}
