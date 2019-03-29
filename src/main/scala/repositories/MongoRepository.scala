package repositories

import reactivemongo.api.BSONSerializationPack.Writer
import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.api.commands.MultiBulkWriteResult

import scala.concurrent.{ExecutionContext, Future}


abstract class MongoRepository[T](reactiveMongo: ReactiveMongo)
                                 (implicit executionContext: ExecutionContext) {

  def collectionName: String

  protected def insertMany(documents: Seq[T])(implicit writer: Writer[T]): Future[MultiBulkWriteResult] = {
    val collection: Future[BSONCollection] = reactiveMongo.db.map(_.collection(collectionName))
    collection.flatMap(_.insert.many(documents))
  }
}