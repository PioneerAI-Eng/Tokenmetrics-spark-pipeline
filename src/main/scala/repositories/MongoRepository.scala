package repositories

import models.ModelWithId
import reactivemongo.api.BSONSerializationPack.Writer
import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.api.commands.MultiBulkWriteResult
import reactivemongo.bson.BSONDocument

import scala.concurrent.{ExecutionContext, Future}


abstract class MongoRepository[T <: ModelWithId](reactiveMongo: ReactiveMongo)
                                                (implicit executionContext: ExecutionContext) {

  def collectionName: String

  protected def insertMany(documents: Seq[T])(implicit writer: Writer[T]): Future[MultiBulkWriteResult] = {
    val collection: Future[BSONCollection] = reactiveMongo.db.map(_.collection(collectionName))

    for {
      resolvedCollection <- collection
      updateBuilder <- Future(resolvedCollection.update(ordered = true))
      updates <- Future.sequence(documents.map(doc => {
        updateBuilder.element(
          q = BSONDocument("_id" -> doc._id),
          u = doc,
          upsert = true,
          multi = false
        )
      }))
      result <- updateBuilder.many(updates)
    } yield result

  }
}