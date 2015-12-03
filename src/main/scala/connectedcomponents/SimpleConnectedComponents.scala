package connectedcomponents

import connectedcomponents.check.ConnectedComponentsCompare
import connectedcomponents.core.{ConnectedComponents2, ConnectedComponents}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, graphx}


/**
 * Created by grzegorz.miejski on 15/11/15.
 */
object SimpleConnectedComponents {
  def main(args: Array[String]) {

    println(args)

    val conf = new SparkConf().setAppName("Simple PageRank").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val vertexes: RDD[(VertexId, Long)] = prepareVertexRDD(sc)
    val edges: RDD[(VertexId, VertexId)] = prepareEdgesRDD(sc)

    val edgesRdd = edges.flatMap(s => List(Edge(s._1, s._2, 123),Edge(s._2, s._1, 123)))

    val graph = Graph(vertexes, edgesRdd, Long.MaxValue)

    val connectedComponentsGraph: Graph[Long, Int] = ConnectedComponents2.connectedComponentsGraph(sc, graph)
    val finalGroups = ConnectedComponents.groupVertexes(connectedComponentsGraph).collect()
    val finalGroupsSpark = ConnectedComponents.groupVertexes(graph.connectedComponents()).collect()


    ConnectedComponentsCompare.compare(finalGroups, finalGroupsSpark)

    val connectedComponentsGraph2: Graph[Long, Int] = ConnectedComponents.connectedComponentsGraph(sc, graph.vertices, graph.edges)
    val finalGroups2 = ConnectedComponents.groupVertexes(connectedComponentsGraph2).collect()

    ConnectedComponents.compareVertices(graph.connectedComponents(), connectedComponentsGraph)
    sc.stop()
  }

  def prepareEdgesRDD(sc: SparkContext): RDD[(VertexId, VertexId)] = {
    val edges: RDD[Edge[String]] = sc.parallelize(Array(Edge(0L, 1L, "a"), Edge(1L, 2L, "b"), Edge(2L, 0L, "c"), Edge(6L, 7L, "d"), Edge(4L, 5L, "e"), Edge(1L, 9L, "f"), Edge(8L, 1L, "g"), Edge(9L, 10L, "h")))

    edges.flatMap(s => Array((s.srcId, s.dstId), (s.dstId, s.srcId)))
  }

  def prepareVertexRDD(sc: SparkContext, numberOfVertices: Long = 10): RDD[(graphx.VertexId, Long)] = {
    val vertexesIds = Range.apply(0, numberOfVertices.toInt).toArray
    sc.parallelize(vertexesIds.map(x => (x.toLong, x.toLong)))
  }
}
