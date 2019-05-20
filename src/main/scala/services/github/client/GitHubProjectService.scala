package services.github.client

import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeUnit.SECONDS

import akka.actor.{ActorSystem, Terminated}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.pattern._
import akka.stream._
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Keep, Merge, Partition, RestartSource, Sink, Source}
import akka.util.Timeout
import com.google.inject.Inject
import models.GitHubRepositoryProtocol._
import models.PageInfoProtocol._
import models.{GitHubRepository, PageInfo}
import repositories.github.GitHubProjectRepository
import services.github.client.GitHubRequestComposer.GraphQLQuery
import spray.json._
import utils.Logger

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class GitHubProjectService @Inject()(gitHubProjectRepository: GitHubProjectRepository) extends Logger {

  def fetchRepositoriesWithGraphQL(body: String, totalCount: Int, elementsPerPage: Int) = {

    //todo: fix bug: After fetching some count of repositories from GitHub
    // throw exception related to connections created with ReactiveMongo
    implicit val as: ActorSystem = ActorSystem("GitHub-ActorSystem")
    implicit val mat: ActorMaterializer = ActorMaterializer()
    implicit val ec: ExecutionContext = as.dispatchers.lookup("github-dispatcher")

    val aggregate = RestartSource.withBackoff(
      minBackoff = new FiniteDuration(3, TimeUnit.SECONDS),
      maxBackoff = new FiniteDuration(5, TimeUnit.SECONDS),
      randomFactor = 0.2
    ){ () =>

      implicit val timeout: Timeout = Timeout(new FiniteDuration(10, TimeUnit.SECONDS))

      val source = Source.single[String](body)

//      val sharedKillSwitch: SharedKillSwitch = KillSwitches.shared("GitHub-Repositories-Fetcher")

      val gitHubRequestComposer = as.actorOf(GitHubRequestComposer.props(totalCount, elementsPerPage))

      def checkErrorMess(mess: JsObject): Boolean = {
        mess.getFields("errors").nonEmpty &&
          mess.getFields("errors").toString.indexOf("timedout") > -1
      }

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
        val httpResponseChecker = builder.add {
          Flow[HttpResponse]
            .map(res => {
              log.error(s"response ===> $res")
              if (res.status.intValue == 502)
                throw new InternalError(s"Internal server error, ${res.httpMessage}")
              else res
            })
        }
        val deserialization = builder.add {
          Flow[HttpResponse]
            .mapAsync(1)(res => Unmarshal(res.entity).to[String])
            .map {
              entity =>

                if (checkErrorMess(entity.parseJson.asJsObject)) {
                  throw new RuntimeException("Timed out waiting for a response from the data source")
                }

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
//        val httpRequestPartitioner = builder.add {
//          Partition[Option[HttpRequest]](
//            outputPorts = 2,
//            partitioner = {
//              case Some(_) => 1
//              case None => 0
//            }
//          )
//        }
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
//        val cancelledSink = {
//          Flow[Option[HttpRequest]].map(_ => sharedKillSwitch.shutdown).to(Sink.ignore)
//        }

        /*
                                   httpRequestPartitioner ~> cancelledSink
        M ~> composeHttpRequest ~> httpRequestPartitioner ~> sendRequest ~> httpResponsePartitioner ~> Sink.ignore
                                                                            httpResponsePartitioner ~> deserialization ~> B
        M                                                       <~                                                        B
        */
//        M ~> composeHttpRequest ~> httpRequestPartitioner
//        httpRequestPartitioner.out(0) ~> cancelledSink
//        httpRequestPartitioner.out(1) ~> sendRequest ~> httpResponsePartitioner
//        httpResponsePartitioner.out(0) ~> httpResponseChecker ~> Sink.ignore
//        httpResponsePartitioner.out(1) ~> deserialization ~> B

        M ~> composeHttpRequest ~> sendRequest ~> httpResponsePartitioner
                httpResponsePartitioner.out(0) ~> httpResponseChecker ~> Sink.ignore
                httpResponsePartitioner.out(1) ~> deserialization ~> B

        B.out(0).map(res => GraphQLQuery(body, Some(res._1.endCursor))) ~> M.in(0)

        FlowShape(M.in(1), B.out(1))
      }

      source
        .map(_ => GraphQLQuery(body))
        .via(graph)
//        .via(sharedKillSwitch.flow)
        .map(_._2)
        .mapAsync(1)(gitHubProjectRepository.insertMany)

//      as.whenTerminated

    }

    val killSwitch = aggregate
      .viaMat(KillSwitches.single)(Keep.right)
      .toMat(Sink.foreach(event => println(s"Got event: $event")))(Keep.left)
      .run()

    aggregate.runWith {
      Sink.onComplete {
        case Success(value) => {
          log.info("Terminate the stream.")
//          as.terminate
//          killSwitch.shutdown()
        }
        case Failure(exception) => log.info("Failure - restart")
      }
    }

    as.whenTerminated

  }
}
