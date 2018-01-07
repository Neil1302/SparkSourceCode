
package examples

import org.apache.spark.{ SparkConf, SparkContext, HashPartitioner }
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import scala.Iterator

/**
 * Test code for various types of joins on RDD.
 * For the following operations the partitioners are passed 
 * from the parent RDD to the transformed RDD. 
 */

object TestJoins {
	def main(args: Array[String]): Unit = {
		val sc = new SparkContext(new SparkConf().setAppName("TestJoinJob"))

		val x = sc.parallelize(List((1, 2), (1, 3), (2, 3), (2, 4))).partitionBy(new HashPartitioner(2)).cache
		val y = sc.parallelize(List((2, 5), (2, 6))).partitionBy(new HashPartitioner(2)).cache

		inspectRDD(x)
		inspectRDD(y)

		println(">>> joining x with y")
		val joinRDD = x.join(y).cache
		joinRDD.collect().foreach(println)
		inspectRDD(joinRDD)

		println(">>> left outer join of x with y")
		val leftJoin = x.leftOuterJoin(y).cache
		leftJoin.collect().foreach(println)
		inspectRDD(leftJoin)

		println(">>> right outer join of x with y")
		val rightJoin = x.rightOuterJoin(y).cache
		rightJoin.collect().foreach(println)
		inspectRDD(rightJoin)
	}
	
	def inspectRDD[T](rdd: RDD[T]): Unit = {
		
		println(">>> Partition length...")
		rdd.mapPartitions(f => Iterator(f.length), true).foreach(println)
		
		println(">>> Partition data...")
		rdd.foreachPartition(f => f.foreach(println))
	}
}