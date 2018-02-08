package examples.streaming

import org.apache.spark.streaming.{ StreamingContext, Duration }
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel

/**
 * Test code for the PairDStreamFunction.reduceByKeyAndWindow(...).
 * Listens to socket text stream on host=localhost, port=9999.
 * Tokenizes the incoming stream into (words, no. of occurrences).
 * Checkpoint dir created in HDFS.
 * Chekpointing frequency every 10s.
 * Checkpointing is mandatory for cumulative stateful operations.
 * By default persistence is enabled for stateful operations.
 */

object TestReduceByKeyAndWindow {
  val checkpointDir: String = "hdfs://localhost:9000/user/hduser/spark-chkpt"

  def main(args: Array[String]): Unit = {

    val ssc = StreamingContext.getOrCreate(checkpointDir, createFunc _)
    ssc.start()
    ssc.awaitTermination()
  }

  def createFunc(): StreamingContext = {

    val ssc = new StreamingContext(new SparkConf().setAppName("TestReduceByKeyAndWindowJob"),
      Duration(2000))

    ssc.checkpoint(checkpointDir)

    /* As the application is a simple test we are overriding the default 
		Receiver's setting of StorageLevel.MEMORY_AND_DISK_SER_2 */
    val receiver = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_ONLY_SER)

    val tokenToMonitor = receiver.flatMap(_.split(" ")).map(token => (token, 1))

    val reducedOverWindow = tokenToMonitor
      // calculate the occurrence of the token over the 
      // last 3s and compute the results every 2s
      .reduceByKeyAndWindow(_ + _, // adding elements in the new batches entering the window 
        _ - _, // removing elements from the new batches exiting the window 
        Duration(4000), Duration(2000))

    reducedOverWindow.checkpoint(Duration(10000))
    reducedOverWindow.print()

    ssc

  }
}