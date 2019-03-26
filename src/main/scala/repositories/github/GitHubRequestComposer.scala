package repositories.github

import akka.actor.{Actor, ActorLogging, Props}
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.RawHeader
import repositories.github.GitHubRequestComposer._

object GitHubRequestComposer {

  //todo: add personal GitHub token
  private val headers = RawHeader("Authorization", "Bearer ") ::
    RawHeader("Accept", "application/vnd.github.hawkgirl-preview") :: Nil
  private val uri: Uri = Uri("https://api.github.com/graphql")
  private val httpRequest = HttpRequest(uri = uri, method = HttpMethods.POST).withHeaders(headers)

  def props(totalCount: Int, elementsPerPage: Int = 10): Props = {
    Props(new GitHubRequestComposer(totalCount, elementsPerPage))
  }

  //todo: add checking for hasNext page
  case class GraphQLQuery(body: String, cursor: Option[String] = None)

  case class ReduceElementsPerPage(httpRequest: HttpRequest)

}

class GitHubRequestComposer(totalCount: Int, elementsPerPage: Int) extends Actor with ActorLogging {

  override def receive: Receive = handleIncomingMessages(totalCount, elementsPerPage)

  private def handleIncomingMessages(totalCount: Int, elementsPerPage: Int): Receive = {

    case query: GraphQLQuery => {
      if (totalCount == 0) {
        log.info(s"All elements were fetched. Total count = $totalCount")
        sender ! None
      }
      else {
        val perPage = if (elementsPerPage > totalCount) totalCount else elementsPerPage

        val replacement = query.cursor match {
          case Some(value) => s"first: $perPage , after: " + """\\"""" + value + """\\""""
          case _ => s"first: $perPage"
        }
        val body = query.body.replaceFirst("(first)(.*?)[0-9]+", replacement)
        log.debug(s"GraphQL body = $body")
        context.become(handleIncomingMessages(totalCount - perPage, perPage))
        sender ! Some(httpRequest.withEntity(HttpEntity(ContentTypes.`application/json`, body)))
      }
    }

    case reduceElementsPerPage: ReduceElementsPerPage => { //todo: if we catch timeout exception
      //todo: stop stream if elementsPerPage == 1
    }

  }
}
