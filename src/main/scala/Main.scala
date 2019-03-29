import com.google.inject.Guice
import modules.{AkkaModule, ConfigModule, DBModule}
import org.graphframes.GraphFrame
import services.github.client.GitHubProjectService
import services.github.spark.GitHubGraphXService
import services.spark.SparkMongoService

import scala.concurrent.ExecutionContext

object Main extends App {

  //todo: connect drunk library to work with Sangria graphql
  private val body = "{ \"query\": \"query { search(query: Java, type: REPOSITORY, first: 1) { pageInfo { hasNextPage startCursor endCursor } edges { node { ... on Repository { id name description createdAt stargazers { totalCount } forkCount updatedAt dependencyGraphManifests { totalCount nodes { dependencies { edges { node { packageName requirements } } } } } } } } } } \"}"
  private val injector = Guice.createInjector(new ConfigModule, new AkkaModule, new DBModule)

  implicit val ec = injector.getInstance(classOf[ExecutionContext])
  val gitHubRepositoryService: GitHubProjectService = injector.getInstance(classOf[GitHubProjectService])
  gitHubRepositoryService.fetchRepositoriesWithGraphQL(body, 10, 5).onComplete {
    _ =>
      val (dataFrame, _) = injector.getInstance(classOf[SparkMongoService]).loadData
      val graphFrame: GraphFrame = injector.getInstance(classOf[GitHubGraphXService]).createGraphFrame(dataFrame)
    //todo: connect Neo4j to the application
    //todo: convert graphFrame into GraphX and store into Neo4j
    //todo: add ability to update a graph in Neo4j
  }
}
