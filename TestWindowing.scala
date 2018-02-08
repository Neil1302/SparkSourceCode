package examples.streaming

import org.apache.spark.streaming.{ StreamingContext, Seconds }
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel

/**
 * Sample for windowing DStream transformations. Monitors the number of times a 
 * token appears in a window interval of 3s with slide interval of 2s.
 */
object TestWindowing {
	def main(args: Array[String]): Unit = {

		if (args.length != 3) {
			println("Usage: examples.streaming.TestWindowing host port token_to_monitor")
			sys.exit(-1)
		}
		
		val ssc = new StreamingContext(new SparkConf().setAppName("TestWindowingJob"), Seconds(1))
		
		/* As the application is a simple test we are overriding the default 
		Receiver's setting of StorageLevel.MEMORY_AND_DISK_SER_2 */
		val msg = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_ONLY)
		
		val tokenToMonitor = msg.flatMap(_.split(" "))
									.filter(_.equals(args(2)))
									// calculate the occurrence of the token over the 
									// last 3s and compute the results every 2s
									.window(Seconds(3), Seconds(2))
		
		println(">>> Computing every 2s, in the last 3s " + args(2) + " occurred.")
		
		tokenToMonitor.count.print()
		
		ssc.start()
		ssc.awaitTermination()
		ssc.stop()
	}
}