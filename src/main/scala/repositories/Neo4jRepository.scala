package repositories

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.graphframes.GraphFrame
import org.neo4j.spark.Neo4j

class Neo4jRepository {

  def getSimpleGraph(graphFrame: GraphFrame, sparkNeoSession: SparkSession): Unit = {
    val sc: SparkContext = sparkNeoSession.sparkContext
    val neoEntry: Neo4j = Neo4j(sc)
    val rowRdd = neoEntry.cypher("MATCH (n) RETURN n LIMIT 25").loadRowRdd
    println("RESULT COUNT " + rowRdd.count())
//    val graphToSave = graphFrame.toGraphX
//    println(graphToSave.numEdges)
//    neoEntry.saveGraph(graphFrame.toGraphX)
  }

}
