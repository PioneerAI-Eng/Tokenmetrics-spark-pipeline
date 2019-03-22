package models

import spray.json.{DefaultJsonProtocol, RootJsonFormat}

case class PageInfo(startCursor: String, endCursor: String, hasNextPage: Boolean)

object PageInfoProtocol extends DefaultJsonProtocol {
  implicit val pageInfoFormat: RootJsonFormat[PageInfo] = jsonFormat3(PageInfo)
}