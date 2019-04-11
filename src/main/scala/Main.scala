import com.google.inject.Guice
import modules.{AkkaModule, ConfigModule, DBModule}
import org.graphframes.GraphFrame
import repositories.Neo4jRepository
import services.github.client.GitHubProjectService
import services.github.spark.GitHubGraphXService
import services.spark.{SparkContextConf, SparkMongoService}

import scala.concurrent.ExecutionContext

object Main extends App {

  //todo: connect drunk library to work with Sangria graphql
  private val body = "{ \"query\": \"query { search(query: Java, type: REPOSITORY, first: 1) { pageInfo { hasNextPage startCursor endCursor } edges { node { ... on Repository { id name description createdAt stargazers { totalCount } forkCount updatedAt dependencyGraphManifests { totalCount nodes { dependencies { edges { node { packageName requirements } } } } } } } } } } \"}"
  private val injector = Guice.createInjector(new ConfigModule, new AkkaModule, new DBModule)

  implicit val ec = injector.getInstance(classOf[ExecutionContext])
  val gitHubRepositoryService: GitHubProjectService = injector.getInstance(classOf[GitHubProjectService])
  gitHubRepositoryService.fetchRepositoriesWithGraphQL(body, 10, 5).onComplete {
    _ =>
      //todo: change configuration when using Spark on a cluster
      val sparkMongoSession = injector.getInstance(classOf[SparkContextConf]).getSparkSession("local", "MongoSession")
      val sparkNeoSession = injector.getInstance(classOf[SparkContextConf]).getSparkSession("local", "NeoSession")
      val dataFrame = injector.getInstance(classOf[SparkMongoService]).loadData(sparkMongoSession)
      val graphFrame: GraphFrame = injector.getInstance(classOf[GitHubGraphXService]).createGraphFrame(dataFrame)
      val loadResult: Unit = injector.getInstance(classOf[Neo4jRepository]).saveGraph(graphFrame, sparkNeoSession)

  }
}
