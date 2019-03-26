package repositories

import reactivemongo.api.{DefaultDB, MongoConnection, MongoDriver}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

class ReactiveMongo(databaseUri: String, databaseName: String)
                   (implicit ec: ExecutionContext) {

  private val driver: MongoDriver = MongoDriver()

  def connection: Future[MongoConnection] = {
    val parsedUri: Try[MongoConnection.ParsedURI] = MongoConnection.parseURI(databaseUri)
    val connection = parsedUri.map(driver.connection)
    Future.fromTry(connection)
  }

  def db: Future[DefaultDB] = connection.flatMap(_.database(databaseName))
}
