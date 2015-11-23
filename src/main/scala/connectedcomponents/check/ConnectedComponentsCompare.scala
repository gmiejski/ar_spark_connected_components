package connectedcomponents.check

/**
 * Created by grzegorz.miejski on 21/11/15.
 */
object ConnectedComponentsCompare {
  def compare(finalGroups: Array[Seq[Long]], finalGroupsSpark: Array[Seq[Long]]) = {
    println(s"My connected components count: ${finalGroups.length}")
//    finalGroups.foreach(println)
    println(s"Original connected components count: ${finalGroupsSpark.length}")
//    finalGroupsSpark.foreach(println)
  }
}
