package connectedcomponents

import connectedcomponents.check.ConnectedComponentsCompare
import connectedcomponents.core.ConnectedComponents
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by grzegorz.miejski on 21/11/15.
 */
object ConnectedComponentsFacebookGraph {

  def main(args: Array[String]) {


    val conf = new SparkConf().setAppName("Simple PageRank").setMaster("local[4]")
    val sc = new SparkContext(conf)


    val edges = sc.textFile("src/main/resources/facebook.edges")


    val cachedEdgesRDD = edges.map(s => s.split(" ")).map(s => (s.apply(0).toLong, s.apply(1).toLong)).cache()
    val verticesForAlgorithm: RDD[(VertexId, VertexId)] = cachedEdgesRDD.flatMap(s => List(s._1, s._2)).distinct().map(s => (s, s))
    val edgesForAlgorithm = cachedEdgesRDD.map(s => Edge(s._1, s._2, 123123))

    val connectedComponentsGraph: Graph[Long, Int] = ConnectedComponents.connectedComponentsGraph(sc, verticesForAlgorithm, edgesForAlgorithm)

    val graph = Graph(verticesForAlgorithm, edgesForAlgorithm)
    val v1 = connectedComponentsGraph.vertices
    if (args.length > 1) {

      println(args.apply(0))
      val checkIfOk = args.apply(0).toBoolean
      if (checkIfOk) {
        ConnectedComponents.compareVertices(graph.connectedComponents(), connectedComponentsGraph)
        compareWithOriginalConnectedComponents(graph, connectedComponentsGraph, v1)
      }
    } else {
      ConnectedComponents.compareVertices(graph.connectedComponents(), connectedComponentsGraph)
      compareWithOriginalConnectedComponents(graph, connectedComponentsGraph, v1)
    }

    cachedEdgesRDD.unpersist()
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
