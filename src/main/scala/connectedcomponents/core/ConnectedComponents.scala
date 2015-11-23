package connectedcomponents.core

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD

/**
 * Created by grzegorz.miejski on 15/11/15.
 */
object ConnectedComponents {


  def connectedComponentsGraph(sc: SparkContext, startingVertexes: RDD[(VertexId, Long)], startingEdges: RDD[Edge[Int]]): Graph[Long, Int] = {

    val start = System.currentTimeMillis()

    var shouldContinue = true
    var iterations = 0

    var vertexes = startingVertexes
    val edges = startingEdges.flatMap(s => List((s.srcId, s.dstId), (s.dstId, s.srcId)))

    while (shouldContinue) {
      iterations += 1
      println(s"Starting iteration: $iterations")

      val updatedValues = edges.join(vertexes)
      val updates = edges.join(vertexes).map(s => s._2)

      val smallest_numbers = updates.reduceByKey(math.min)
      val newVertexes = vertexes.join(smallest_numbers).map(s => (s._1, math.min(s._2._1, s._2._2)))
      shouldContinue = changesDetected(vertexes, newVertexes)
      vertexes = newVertexes
      println(updates)
    }

    println(s"Finished after $iterations iterations")

    val graph = Graph(vertexes, startingEdges, 1111L)
    val end = System.currentTimeMillis()

    println(s"My connected components process took ${end - start} miliseconds")
    graph
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
