package connectedcomponents.core

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{EdgeTriplet, Graph, VertexId}
import org.apache.spark.rdd.RDD

// Import random graph generation library

/**
 * Created by grzegorz.miejski on 15/11/15.
 */
object ConnectedComponents2 {


  def connectedComponentsGraph(sc: SparkContext, graph: Graph[Long, Int]): Graph[Long, Int] = {

    val start = System.currentTimeMillis()

    var shouldContinue = true
    var iterations = 0

    val initialMapping: (VertexId, VertexId, VertexId) => VertexId = (vertexId, data, initial) => {
      math.min(data, initial)
    }

    val sendMessage: (EdgeTriplet[VertexId, Int]) => Iterator[(VertexId, VertexId)] = (edge) => {
      if (edge.srcAttr < edge.dstAttr) {
        Iterator((edge.dstId, edge.srcAttr))
      } else if (edge.srcAttr > edge.dstAttr) {
        Iterator((edge.srcId, edge.dstAttr))
      } else {
        Iterator.empty
      }
    }

    val mergeMessage: (VertexId, VertexId) => VertexId = (v1, v2) => {
      math.min(v1, v2)
    }

    val resultGraph = graph.pregel(Long.MaxValue)(initialMapping, sendMessage, mergeMessage)

    val end = System.currentTimeMillis()

    println(s"My connected components process took ${end - start} miliseconds")
    resultGraph
  }

  def changesDetected(vertexes: RDD[(VertexId, Long)], newVertexes: RDD[(VertexId, Long)]): Boolean = {
    vertexes.join(newVertexes).filter(s => s._2._1 != s._2._2).count() > 0
  }

  def groupVertexes(graph: Graph[Long, Int]): RDD[Seq[Long]] = {
    graph.vertices.map(x => (x._2, x._1)).groupByKey().map(x => x._2.toSeq.sorted)
  }

  def compareVertices(g1: Graph[Long, Int], g2: Graph[Long, Int]) = {

    val v1: RDD[(Long, Long)] = g1.vertices.map(s => (s._1, s._2))
    val v2: RDD[(Long, Long)] = g2.vertices.map(s => (s._1, s._2))
    v1.join(v2).foreach(matchingVertex => {
      if (!matchingVertex._1.equals(matchingVertex._1)) {
        throw new IllegalStateException()
      }

    })


  }


}
