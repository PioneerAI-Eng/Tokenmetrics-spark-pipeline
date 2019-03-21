import akka.actor.ActorSystem
import com.google.inject.Guice
import modules.{AkkaModule, ConfigModule, DBModule}
import services.github.GitHubRepositoryService
import utils.LoggingConfig

object Main extends App with LoggingConfig {

  private val body = "{ \"query\": \"query { search(query: Java, type: REPOSITORY, first: 1) { pageInfo { hasNextPage startCursor endCursor } edges { node { ... on Repository { id name description createdAt stargazers { totalCount } forkCount updatedAt dependencyGraphManifests { totalCount nodes { dependencies { edges { node { packageName requirements } } } } } } } } } } \"}"
  private val injector = Guice.createInjector(new ConfigModule, new AkkaModule, new DBModule)

  val gitHubRepositoryService: GitHubRepositoryService = injector.getInstance(classOf[GitHubRepositoryService])
  gitHubRepositoryService.fetchRepositoriesWithGraphQL(body, 10, 5)

  injector.getInstance(classOf[ActorSystem]).terminate
}
