package services.github.spark

import com.google.inject.Inject
import models.{Dependency, GitHubRepository}
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, Row}
import org.graphframes.GraphFrame
import services.spark.SparkMongoService

import scala.collection.mutable

class GitHubGraphXService @Inject()(sparkMongoService: SparkMongoService) {

  /**
    * Convert DataFrame with GitHubRepository data into GraphFrame
    *
    * @param dataFrame an instance of DataFrame
    * @return an instance of GraphFrame
    */
  def createGraphFrame(dataFrame: DataFrame): GraphFrame = {
    implicit val gitHubRepositoryEncoder: Encoder[GitHubRepository] = Encoders.product[GitHubRepository]
    implicit val dependencyEncoder: Encoder[Dependency] = Encoders.product[Dependency]
    implicit val stringEncoder: Encoder[String] = Encoders.STRING
    implicit val tupleEncoders: Encoder[(String, String, String)] = Encoders.tuple(Encoders.STRING, Encoders.STRING, Encoders.STRING)

    val projects = dataFrame
      .select("_id", "dependencies")
      .filter(""" size(dependencies) != 0 """)

    //Create vertices and edges in order to create a graph structure.
    val vertices: DataFrame = projects
      .flatMap(_.getAs[mutable.WrappedArray[Row]]("dependencies").map(_.getAs[String]("packageName")))
      .distinct
      .toDF("id")
    //in order to view a result table use vertices.show()
    val edges: DataFrame = projects.flatMap {
      row =>
        val id = row.getAs[String]("_id")
        val packages = row
          .getAs[mutable.WrappedArray[Row]]("dependencies")
          .map(_.getAs[String]("packageName"))
        foldIntoPairs(packages.toList).map(t => (t._1, t._2, id))
    }.toDF("src", "dst", "projects") //todo: group by technologies
    //id order to view a result table use edges.show()

    GraphFrame(vertices, edges)
  }

  def foldIntoPairs[T](list: List[T]): List[(T, T)] = list match {
    case head :: tail => tail.map(el => (head, el)) ++ foldIntoPairs(tail)
    case _ => Nil
  }
}
