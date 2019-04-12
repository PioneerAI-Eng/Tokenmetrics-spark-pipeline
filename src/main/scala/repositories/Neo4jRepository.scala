package repositories

import models.RelationProp
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.graphframes._
import org.neo4j.spark._
import utils.Logger

class Neo4jRepository extends Logger {

  implicit val relationEncoder: Encoder[RelationProp] = Encoders.product[RelationProp]
  implicit val tuple2Encoders: Encoder[(VertexId, String)] = Encoders.product[(VertexId, String)]
  implicit val long2Encoders: Encoder[(Long, Long, RelationProp)] = Encoders.product[(Long, Long, RelationProp)]
  implicit val edgeEncoders: Encoder[Edge[String]] = Encoders.product[Edge[String]]

  def saveGraph(graphFrame: GraphFrame, sparkNeoSession: SparkSession): Unit = {
    val sc: SparkContext = sparkNeoSession.sparkContext

    val neo: Neo4j = Neo4j(sc)

    val pattern = neo.pattern(("package", "id"), ("rel", "id"), ("package", "id"))

    val verticeRdd: RDD[(VertexId, String)] = {
      val array = graphFrame.vertices.map(ver => {
        val id = ver.getAs[VertexId]("id")
        val packageName = ver.getAs[String]("package")
        (id, packageName)
      }).collect()
      sc.parallelize(array)
    }

    val edgesRdd: RDD[Edge[String]] = {
      val array = graphFrame.edges.map(edg => {
        val src = edg.getAs[Long]("src")
        val dst = edg.getAs[Long]("dst")
        val prop = "relationship"
        Edge[String](src, dst, prop)
      }).collect()
      sc.parallelize(array)
    }

    val graph1 = Graph(verticeRdd, edgesRdd)

    neo.saveGraph(graph1, "name", neo.pattern, true)

  }

}
