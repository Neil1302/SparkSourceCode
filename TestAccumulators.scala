package examples

import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.rdd.RDD

/**
 * Test code for accumulators.
 * Use cases for accumulators are mostly as counters or for diagnostic 
 * information.
 * For more details visit, 
 * http://spark.apache.org/docs/latest/programming-guide.html#understanding-closures-a-nameclosureslinka
 *
 */
object TestAccumulators {
	def main(args: Array[String]): Unit = {

		val sc = new SparkContext(new SparkConf().setAppName("TestAccumulatorsJob"))

		val logRDD = sc.textFile("file:/media/linux-1/spark-dev/data/sample.log")

		usingAccumulators(sc, logRDD)

		usingRDDTransformations(sc, logRDD)
	}

	def usingAccumulators(sc: SparkContext, rdd: RDD[String]): Unit = {
		val errorLines = sc.accumulator(0, "Error_Logs")
		val infoLines = sc.accumulator(0, "Info_Logs")
		val warnLines = sc.accumulator(0, "Warn_Logs")
		val totalLines = sc.accumulator(0, "Total_Lines")

		/* It is recommended to use accumulators inside actions only. This guarantees 
		 * that the update is applied only once in spite of job restarts.
		 * For accumulators inside transformations, the lineage may be recomputed 
		 * several times, hence Spark does not recommended accumulators inside 
		 * transformations.
		 * 
		 * The code below uses accumulators inside an action.
		 */
		rdd.foreach { line =>
			if (line.length() > 0) totalLines += 1
			if (line.startsWith("error:")) errorLines += 1
			else if (line.startsWith("info:")) infoLines += 1
			else if (line.startsWith("warn:")) warnLines += 1
		}

		println(s">>> [Using Accumulators] Total: ${totalLines.value}, Error: ${errorLines.value}, Warnings: ${warnLines.value}, Info: ${infoLines.value}")
	}

	def usingRDDTransformations(sc: SparkContext, rdd: RDD[String]): Unit = {
		val errorLines = rdd.filter(_.startsWith("error:")).count()
		val infoLines = rdd.filter(_.startsWith("info:")).count()
		val warnLines = rdd.filter(_.startsWith("warn:")).count()

		println(s">>> [Using RDD Transformations] Error: $errorLines, Warnings: $warnLines, Info: $infoLines")
	}
}
