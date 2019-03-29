import com.google.inject.Guice
import modules.{AkkaModule, ConfigModule, DBModule}
import services.github.GitHubProjectService
import services.github.spark.GitHubGraphXService
import services.spark.SparkMongoService

import scala.concurrent.ExecutionContext

object Main extends App {

  private val body = "{ \"query\": \"query { search(query: Java, type: REPOSITORY, first: 1) { pageInfo { hasNextPage startCursor endCursor } edges { node { ... on Repository { id name description createdAt stargazers { totalCount } forkCount updatedAt dependencyGraphManifests { totalCount nodes { dependencies { edges { node { packageName requirements } } } } } } } } } } \"}"
  private val injector = Guice.createInjector(new ConfigModule, new AkkaModule, new DBModule)

  implicit val ec = injector.getInstance(classOf[ExecutionContext])
  val gitHubRepositoryService: GitHubProjectService = injector.getInstance(classOf[GitHubProjectService])
  gitHubRepositoryService.fetchRepositoriesWithGraphQL(body, 10, 5).onComplete {
    _ =>
      val (dataFrame, _) = injector.getInstance(classOf[SparkMongoService]).loadData
      injector.getInstance(classOf[GitHubGraphXService]).createGraphFrame(dataFrame)
      //After that when we've created GraphFrame object we can store this graph into Neo4j database.
  }
}
