package services.github.spark

import scala.language._
import com.google.inject.Inject
import models.{Dependency, GitHubRepository, RelationProp}
import org.apache.spark.sql._
import org.graphframes.GraphFrame
import services.spark.SparkMongoService

import scala.collection.mutable
import scala.util.Random

class GitHubGraphXService @Inject()(sparkMongoService: SparkMongoService) {

  /**
    * Convert DataFrame with GitHubRepository data into GraphFrame
    *
    * @param dataFrame an instance of DataFrame
    * @return an instance of GraphFrame
    */
  def createGraphFrame(dataFrame: DataFrame): GraphFrame = {
    //todo: move encoders in an external class
    implicit val gitHubRepositoryEncoder: Encoder[GitHubRepository] = Encoders.product[GitHubRepository]
    implicit val dependencyEncoder: Encoder[Dependency] = Encoders.product[Dependency]
    implicit val relationEncoder: Encoder[RelationProp] = Encoders.product[RelationProp]
    implicit val stringEncoder: Encoder[String] = Encoders.STRING
    implicit val tuple2Encoders: Encoder[(Long, String)] = Encoders.product[(Long, String)]
    implicit val longEncoders: Encoder[Long] = Encoders.scalaLong
    implicit val long2Encoders: Encoder[(Long, Long, RelationProp)] = Encoders.product[(Long, Long, RelationProp)]
    implicit val tuple3Encoders: Encoder[(String, String, String)] = Encoders.tuple(Encoders.STRING, Encoders.STRING, Encoders.STRING)

    val centerVertexID = Math.abs(Random.nextLong())

    val projects = dataFrame
      .select("_id", "dependencies")
      .filter(""" size(dependencies) != 0 """)
      .toDF()

    val centralVertex = dataFrame
      .select("name")
      .filter(""" name == "Java" """)
      .distinct()
      .map(name => (centerVertexID, name.getAs[String]("name"))).toDF("id", "package")

    val vertices: DataFrame = {
      val vertexList = projects
          .flatMap(_.getAs[mutable.WrappedArray[Row]]("dependencies").map(dependency => {
              val packageName = Some(dependency.getAs[String]("packageName")).getOrElse("emptyName")
              (Math.abs(Random.nextLong()), packageName)
          }))
          .distinct
          .toDF("id", "package")
      vertexList.union(centralVertex)
    }

    val edges: DataFrame = {
      val dst = vertices.filter(""" package == "Java" """).distinct().map(_.getAs[Long]("id")).first()
      vertices.map {
        ver => {
          val src = ver.getAs[Long]("id")
          (src, dst, RelationProp(1))
        }
      }.toDF("src", "dst", "relationship")
    }

    GraphFrame(vertices, edges)
  }
}
