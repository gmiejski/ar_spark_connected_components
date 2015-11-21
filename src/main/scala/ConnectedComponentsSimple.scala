import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.{SparkConf, SparkContext, graphx}

import org.apache.spark.rdd.RDD

/**
 * Created by grzegorz.miejski on 15/11/15.
 */
object ConnectedComponentsSimple {


  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Simple PageRank").setMaster("local[4]")
    val sc = new SparkContext(conf)


    val vertexes: RDD[(VertexId, Int)] = prepareVertexRDD(sc)
    val edges: RDD[(VertexId, VertexId)] = prepareEdgesRDD(sc)

    val edgesRdd = edges.map(s => Edge(s._1, s._2, 123))

    val graph = Graph(vertexes, edgesRdd, 11111)

    val connectedComponents: RDD[Seq[VertexId]] = ConnectedComponents.getConnectedComponents(sc, vertexes, edges)

    val finalGroups = connectedComponents.collect()
    sc.stop()
    println(s"Final groups count: ${finalGroups.length}")
    finalGroups.foreach(println)

  }

  def prepareEdgesRDD(sc: SparkContext): RDD[(VertexId, VertexId)] = {
    val edges: RDD[Edge[String]] = sc.parallelize(Array(Edge(0L, 1L, "a"), Edge(1L, 2L, "b"), Edge(2L, 0L, "c"), Edge(6L, 7L, "d"), Edge(4L, 5L, "e"), Edge(1L, 9L, "f"), Edge(8L, 1L, "g"), Edge(9L, 10L, "h")))

    edges.flatMap(s => Array((s.srcId, s.dstId), (s.dstId, s.srcId)))
  }

  def prepareVertexRDD(sc: SparkContext): RDD[(graphx.VertexId, Int)] = {
    val vertexesIds = Range.apply(0, 11).toArray
    sc.parallelize(vertexesIds.map(x => (x.toLong, x)))
  }
}
