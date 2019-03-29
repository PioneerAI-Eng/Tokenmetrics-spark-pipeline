package modules

import com.google.inject.{Provides, Singleton}
import net.codingwell.scalaguice.ScalaModule
import repositories.ReactiveMongo
import repositories.github.GitHubProjectRepository

import scala.concurrent.ExecutionContext

class GitHubModule extends ScalaModule {

  @Provides
  @Singleton
  def gitHubRepository(rm: ReactiveMongo)
                      (implicit ec: ExecutionContext): GitHubProjectRepository = {
    new GitHubProjectRepository(rm)
  }
}
