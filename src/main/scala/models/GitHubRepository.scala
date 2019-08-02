package models

import models.DependencyProtocol._
import spray.json.{DefaultJsonProtocol, JsArray, JsValue, RootJsonReader}

case class GitHubRepository(_id: String,
                            name: String,
                            description: Option[String] = None,
                            createdAt: String,
                            starCount: Int,
                            forkCount: Int,
                            updatedAt: String,
                            dependencies: List[Dependency] = Nil) extends ModelWithId

object GitHubRepositoryProtocol extends DefaultJsonProtocol {

  implicit object GitHubRepositoryFormat extends RootJsonReader[Seq[GitHubRepository]] {
    override def read(json: JsValue): Seq[GitHubRepository] = {
      val repos = json.asInstanceOf[JsArray].elements.map(_.asJsObject.fields("node"))
      repos.map {
        repo =>
          val fields = repo.asJsObject.fields
          val edges = fields("dependencyGraphManifests")
            .asJsObject.fields("nodes")
            .asInstanceOf[JsArray]
            .elements
            .flatMap(_.asJsObject.fields("dependencies").asJsObject.fields("edges").asInstanceOf[JsArray].elements)
          val dependencies = edges.map(_.asJsObject.fields("node").convertTo[Dependency]).toList

          GitHubRepository(
            _id = fields("id").convertTo[String],
            name = fields("name").convertTo[String],
            description = fields("description").convertTo[Option[String]],
            createdAt = fields("createdAt").convertTo[String],
            starCount = fields("stargazers").asJsObject.fields("totalCount").convertTo[Int],
            forkCount = fields("forkCount").convertTo[Int],
            updatedAt = fields("updatedAt").convertTo[String],
            dependencies = dependencies
          )
      }
    }
  }

}