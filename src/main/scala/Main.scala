import akka.actor.ActorSystem
import com.google.inject.Guice
import modules.{AkkaModule, ConfigModule, DBModule}
import services.KafkaServices
import services.github.GitHubProjectService

import scala.concurrent.ExecutionContext

object Main extends App with KafkaServices {

  private val body = "{ \"query\": \"query { search(query: Java, type: REPOSITORY, first: 1) { pageInfo { hasNextPage startCursor endCursor } edges { node { ... on Repository { id name description createdAt stargazers { totalCount } forkCount updatedAt dependencyGraphManifests { totalCount nodes { dependencies { edges { node { packageName requirements } } } } } } } } } } \"}"
  private val injector = Guice.createInjector(new ConfigModule, new AkkaModule, new DBModule)

  implicit val ec = injector.getInstance(classOf[ExecutionContext])

  val gitHubRepositoryService: GitHubProjectService = injector.getInstance(classOf[GitHubProjectService])
  gitHubRepositoryService.fetchRepositoriesWithGraphQL(body, 10, 5).onComplete {
    _ => injector.getInstance(classOf[ActorSystem]).terminate
  }

  //  test drive of transferring data from mongodb to kafka through spark
  //  val resultDataFrame = readFromMongoBySpark()
  //  resultDataFrame._1.forEach(t => sendToKafka("test-data-topic", t._id, t.toString))
  //  resultDataFrame._2.stop


}
