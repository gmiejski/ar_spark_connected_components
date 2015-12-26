package connectedcomponents

import connectedcomponents.check.ConnectedComponentsCompare
import connectedcomponents.core.{ConnectedComponents2, ConnectedComponents}
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.graphx.{Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by grzegorz.miejski on 21/11/15.
 */
object ConnectedComponentsGeneratedGraphPregel {

  def main(args: Array[String]) {

    args.foreach(println)

    if (args.length != 4) {
      println("Need arguments: procesors verticesCount edgesCount [debug]")
      sys.exit(1)
    }

    val processors: Int = args.apply(0).toInt
    val verticesCount: Int = args.apply(1).toInt
    val edgesCount: Int = args.apply(2).toInt
    val debug: Boolean = args.apply(3).toBoolean

    val conf = new SparkConf().setAppName("Simple PageRank")
    val sc = new SparkContext(conf)

    println(s"Graph stats: vertices = $verticesCount, edges = $edgesCount")

    val graph: Graph[Long, Int] = GraphGenerators.rmatGraph(sc, verticesCount, edgesCount).mapVertices((id, _) => id)

    val verticesForAlgorithm: RDD[(VertexId, VertexId)] = graph.vertices.map(s => (s._1, s._2))
    val edgesForAlgorith = graph.edges.map(s => (s.srcId, s.dstId))

    val connectedComponentsGraph: Graph[Long, Int] = ConnectedComponents2.connectedComponentsGraph(sc, graph)

    val v1 = connectedComponentsGraph.vertices

    if (debug) {
      compareWithOriginalConnectedComponents(graph, connectedComponentsGraph, v1)
    }

    val start = System.currentTimeMillis()

    val ccSparkGraph = graph.connectedComponents()
    val end = System.currentTimeMillis()

    println(s"Spark connected components took : ${end - start} milliseconds")

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
