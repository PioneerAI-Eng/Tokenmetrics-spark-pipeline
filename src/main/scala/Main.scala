import akka.actor.ActorSystem
import com.google.inject.Guice
import modules.{AkkaModule, ConfigModule, DBModule}
import services.KafkaServices
import services.github.GitHubRepositoryService
import utils.LoggingConfig

object Main extends App with LoggingConfig with KafkaServices {

  private val body = "{ \"query\": \"query { search(query: Java, type: REPOSITORY, first: 1) { pageInfo { hasNextPage startCursor endCursor } edges { node { ... on Repository { id name description createdAt stargazers { totalCount } forkCount updatedAt dependencyGraphManifests { totalCount nodes { dependencies { edges { node { packageName requirements } } } } } } } } } } \"}"
  private val injector = Guice.createInjector(new ConfigModule, new AkkaModule, new DBModule)

  val gitHubRepositoryService: GitHubRepositoryService = injector.getInstance(classOf[GitHubRepositoryService])
  gitHubRepositoryService.fetchRepositoriesWithGraphQL(body, 10, 5)

//  test drive of transferring data from mongodb to kafka through spark
//  val resultDataFrame = readFromMongoBySpark()
//  resultDataFrame._1.forEach(t => sendToKafka("test-data-topic", t._id, t.toString))
//  resultDataFrame._2.stop

  injector.getInstance(classOf[ActorSystem]).terminate
}
