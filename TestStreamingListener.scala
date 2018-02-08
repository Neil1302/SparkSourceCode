package examples.streaming

/**
 * Using org.apache.spark.streaming.scheduler.StreamListerner
 * trait for listening to streams
 */
import org.apache.spark.streaming.{ StreamingContext, Seconds }
import org.apache.spark.streaming.scheduler.{
	StreamingListener,
	StreamingListenerBatchStarted,
	StreamingListenerBatchCompleted
}
import org.apache.spark.SparkConf

object TestStreamingListener {
	def main(args: Array[String]): Unit = {

		val ssc = new StreamingContext(new SparkConf().setAppName("TestStreamingListenerJob"),
			Seconds(5))

		ssc.addStreamingListener(new MyStreamingListener())

		ssc
			.socketTextStream("localhost", 9999)
			.flatMap(_.split(" "))
			.count()
			.print()

		ssc.start()
		ssc.awaitTermination()
	}
}

class MyStreamingListener extends StreamingListener {

	override def onBatchStarted(batchStarted: StreamingListenerBatchStarted): Unit = {
		println(">>> Batch started...records in batch = " + batchStarted.batchInfo.numRecords)
	}

	override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
		println(">>> Batch completed...time taken (ms) = " + batchCompleted.batchInfo.totalDelay)
	}
}