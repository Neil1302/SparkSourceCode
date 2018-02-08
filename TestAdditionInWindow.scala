package examples.streaming

import org.apache.spark.streaming.{ StreamingContext, Seconds }
import org.apache.spark.SparkConf

/**
 * Finding the sum of the incoming data within the window.
 * Each numbers is delimited by a newline.
 */
object TestAdditionInWindow {
	def main(args: Array[String]): Unit = {
		val ssc = new StreamingContext(new SparkConf().setAppName("TestAdditionJob"), Seconds(1))

		val msg = ssc.socketTextStream("localhost", 9999)

		msg
			.map(data => ("sum", data.toInt))
			.reduceByKey(_ + _)
			.window(Seconds(3), Seconds(2))
			.print()

		ssc.start()
		ssc.awaitTermination()
	}
}