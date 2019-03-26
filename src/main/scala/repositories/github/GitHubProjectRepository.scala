package repositories.github

import com.google.inject.Inject
import models.{Dependency, GitHubRepository}
import reactivemongo.api.commands.MultiBulkWriteResult
import reactivemongo.bson.{BSONDocumentWriter, Macros}
import repositories.{MongoRepository, ReactiveMongo}

import scala.concurrent.{ExecutionContext, Future}

class GitHubProjectRepository @Inject()(rm: ReactiveMongo)
                                       (implicit ec: ExecutionContext) extends MongoRepository[GitHubRepository](rm) {

  override val collectionName: String = "repositories"

  private implicit def repoWriter: BSONDocumentWriter[GitHubRepository] = Macros.writer[GitHubRepository]

  private implicit def dependencyWriter: BSONDocumentWriter[Dependency] = Macros.writer[Dependency]

  def insertMany(documents: Seq[GitHubRepository]): Future[MultiBulkWriteResult] = super.insertMany(documents)
}
