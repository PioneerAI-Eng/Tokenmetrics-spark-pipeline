package services.github

import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeUnit.SECONDS

import actors.GitHubRequestComposer
import actors.GitHubRequestComposer.GraphQLQuery
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.pattern._
import akka.stream._
import akka.stream.alpakka.mongodb.scaladsl.MongoSink
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Merge, Partition, Sink, Source}
import akka.util.Timeout
import com.google.inject.Inject
import com.mongodb.reactivestreams.client.MongoDatabase
import models.GitHubRepositoryProtocol._
import models.PageInfoProtocol._
import models.{Dependency, GitHubRepository, PageInfo}
import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import org.mongodb.scala.bson.codecs.Macros._
import spray.json._
import utils.Logger

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

class GitHubRepositoryService @Inject()(db: MongoDatabase)
                                       (implicit ec: ExecutionContext) extends Logger {

  private val codecRegistry = fromRegistries(fromProviders(classOf[GitHubRepository], classOf[Dependency]), DEFAULT_CODEC_REGISTRY)
  private val repositoriesCollection = db.getCollection("repositories", classOf[GitHubRepository]).withCodecRegistry(codecRegistry)

  def fetchRepositoriesWithGraphQL(body: String, totalCount: Int, elementsPerPage: Int): Unit = {

    implicit val as: ActorSystem = ActorSystem("GitHub-ActorSystem")
    implicit val mat: ActorMaterializer = ActorMaterializer()

    implicit val timeout: Timeout = Timeout(new FiniteDuration(10, TimeUnit.MINUTES))

    val source = Source.single[String](body)
    val sharedKillSwitch: SharedKillSwitch = KillSwitches.shared("GitHub-Repositories-Fetcher")

    val gitHubRequestComposer = as.actorOf(GitHubRequestComposer.props(totalCount, elementsPerPage))

    val graph = GraphDSL.create() { implicit builder =>

      import GraphDSL.Implicits._

      val composeHttpRequest = builder.add {
        Flow[GraphQLQuery].mapAsync(1) {
          query =>
            gitHubRequestComposer.ask(query).mapTo[Option[HttpRequest]]
        }.map {
          r => log.info(s"HTTP request have been composed"); r
        }
      }
      val sendRequest: FlowShape[Option[HttpRequest], HttpResponse] = builder.add {
        Flow[Option[HttpRequest]]
          .collect {
            case Some(httpRequest) => httpRequest
          }
          .map {
            r => log.info(s"Send HTTP request to GitHub. URI: ${r.getUri}"); r
          }
          .mapAsync(1)(request => Http().singleRequest(request))
          .throttle(1, FiniteDuration(1, SECONDS)) //needs in order to not exceed rate limit on GitHub
      }
      val deserialization = builder.add {
        Flow[HttpResponse]
          .mapAsync(1)(res => Unmarshal(res.entity).to[String])
          .map {
            entity =>
              val fields = entity.parseJson
                .asJsObject.fields("data")
                .asJsObject.fields("search")
                .asJsObject.fields
              val repo = fields("edges").convertTo[Seq[GitHubRepository]]
              val pageInfo = fields("pageInfo").convertTo[PageInfo]
              (pageInfo, repo)
          }.map {
          t => log.info(s"Response from GitHub has been converted. PageInfo: ${t._1}"); t
        }
      }
      val httpRequestPartitioner = builder.add {
        Partition[Option[HttpRequest]](
          outputPorts = 2,
          partitioner = {
            case Some(_) => 1
            case None => 0
          }
        )
      }
      val httpResponsePartitioner = builder.add {
        Partition[HttpResponse](
          outputPorts = 2,
          partitioner = p => if (p.status.isSuccess) 1 else 0
        )
      }
      val M = builder.add {
        Merge[GraphQLQuery](2)
      }
      val B = builder.add {
        Broadcast[(PageInfo, Seq[GitHubRepository])](2)
      }
      val cancelledSink = {
        Flow[Option[HttpRequest]].map(_ => sharedKillSwitch.shutdown).to(Sink.ignore)
      }

      /*
                                 httpRequestPartitioner ~> cancelledSink
      M ~> composeHttpRequest ~> httpRequestPartitioner ~> sendRequest ~> httpResponsePartitioner ~> Sink.ignore
                                                                          httpResponsePartitioner ~> deserialization ~> B
      M                                                       <~                                                        B
      */
      M ~> composeHttpRequest ~> httpRequestPartitioner
      httpRequestPartitioner.out(0) ~> cancelledSink
      httpRequestPartitioner.out(1) ~> sendRequest ~> httpResponsePartitioner
      httpResponsePartitioner.out(0) ~> Sink.ignore
      httpResponsePartitioner.out(1) ~> deserialization ~> B

      B.out(0).map(res => GraphQLQuery(body, Some(res._1.endCursor))) ~> M.in(0)

      FlowShape(M.in(1), B.out(1))
    }
    val mongoSink = MongoSink.insertMany(repositoriesCollection)

    source
      .map(_ => GraphQLQuery(body))
      .via(graph)
      .via(sharedKillSwitch.flow)
      .map(_._2)
      .runWith(mongoSink)
      .onComplete {
        _ =>
          log.info("Terminate the stream.")
          as.terminate
      }
  }
}
