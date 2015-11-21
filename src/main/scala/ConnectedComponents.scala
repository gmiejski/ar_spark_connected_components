import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

/**
 * Created by grzegorz.miejski on 15/11/15.
 */
object ConnectedComponents {


  def getConnectedComponents(sc: SparkContext, startingVertexes: RDD[(VertexId, Int)], edges: RDD[(VertexId, VertexId)]): RDD[Seq[VertexId]] = {

    var shouldContinue = true
    var iterations = 0

    var vertexes = startingVertexes

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

    vertexes.map(x => (x._2, x._1)).groupByKey().map(x => x._2.toSeq.sorted)
  }

  def changesDetected(vertexes: RDD[(VertexId, Int)], newVertexes: RDD[(VertexId, Int)]): Boolean = {
    vertexes.join(newVertexes).filter(s => s._2._1 != s._2._2).count() > 0
  }

}
