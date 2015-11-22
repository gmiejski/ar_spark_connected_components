package connectedcomponents

import connectedcomponents.check.ConnectedComponentsCompare
import connectedcomponents.core.ConnectedComponents
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.graphx.{VertexRDD, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by grzegorz.miejski on 21/11/15.
 */
object ConnectedComponentsGeneratedGraph {

  def main(args: Array[String]) {


    val conf = new SparkConf().setAppName("Simple PageRank").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val graph: Graph[Long, Int] = GraphGenerators.rmatGraph(sc, 1000, 200).mapVertices((id, _) => id)

    val verticesForAlgorithm: RDD[(VertexId, VertexId)] = graph.vertices.map(s => (s._1, s._2))
    val edgesForAlgorith = graph.edges.map(s => (s.srcId, s.dstId))

    val connectedComponentsGraph: Graph[Long, Int] = ConnectedComponents.connectedComponentsGraph(sc, verticesForAlgorithm, graph.edges)

    val v1 = connectedComponentsGraph.vertices

    if (args.length > 1) {
      println(args.apply(0))
      val checkIfOk = args.apply(0).toBoolean
      if (checkIfOk) {
        compareWithOriginalConnectedComponents(graph, connectedComponentsGraph, v1)
      }
    } else {
      compareWithOriginalConnectedComponents(graph, connectedComponentsGraph, v1)
    }



    val connectedComponentsGraph2: Graph[Long, Int] = ConnectedComponents.connectedComponentsGraph(sc, verticesForAlgorithm, graph.edges)
    val finalGroups2 = ConnectedComponents.groupVertexes(connectedComponentsGraph2).collect()
    ConnectedComponents.compareVertices(graph.connectedComponents(), connectedComponentsGraph)
    sc.stop()
  }

  def compareWithOriginalConnectedComponents(graph: Graph[VertexId, Int], connectedComponentsGraph: Graph[VertexId, Int], v1: VertexRDD[VertexId]): Unit = {
    println("Comparing with original connected components")
    val v2 = graph.connectedComponents().vertices

    val joinedvvs = v1.join(v2)
    if (joinedvvs.filter(s => !s._2._1.equals(s._2._2)).count() > 0) {
      throw new IllegalStateException()
    }
    val finalGroups = ConnectedComponents.groupVertexes(connectedComponentsGraph).collect()
    val finalGroupsSpark = ConnectedComponents.groupVertexes(graph.connectedComponents()).collect()

    ConnectedComponentsCompare.compare(finalGroups, finalGroupsSpark)
  }
}
