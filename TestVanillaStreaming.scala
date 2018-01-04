package examples.streaming

import org.apache.spark.streaming.{ StreamingContext, Seconds }
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel

/**
 * Use the NetCat server to send messages to this programme.
 * Counts the number of words whose length exceeds 5.
 * This is an example of stateless transformations.
 */
object TestVanillaStreaming {
	def main(args: Array[String]): Unit = {
		
		
		val ssc = new StreamingContext(new SparkConf().setAppName("TestVanillaStreamingJob"), 
				Seconds(5))
		
		/* As the application is a simple test we are overriding the default 
		Receiver's setting of StorageLevel.MEMORY_AND_DISK_SER_2 */
		val msg = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_ONLY)
		
		val wordsMoreThanFiveChars = msg.flatMap(_.split(" ")).filter(_.length > 5)
		
		println(">>> Print number of words having more than 5 characters in the message...")
		wordsMoreThanFiveChars.count().print()
		
		ssc.start()
		
		ssc.awaitTermination()
		
		ssc.stop()
	}
}