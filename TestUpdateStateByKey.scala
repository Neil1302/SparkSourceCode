package examples.streaming

import org.apache.spark.streaming.{StreamingContext, Duration}
import org.apache.spark.SparkConf

/**
 * Listens to socket text stream on host=localhost, port=9999.
 * Tokenizes the incoming stream into (words, no. of occurrences) and tracks the state 
 * of the word using the API 'updateStateByKey'.
 * Checkpoint dir created in HDFS.
 * Chekpointing frequency every 10s.
 */
object TestUpdateStateByKey {
  val checkpointDir: String = "hdfs://localhost:9000/user/hduser/spark-chkpt"

  def main(args: Array[String]): Unit = {
    val ssc = StreamingContext.getOrCreate(checkpointDir, createFunc _)

    ssc.start()
    ssc.awaitTermination()
  }

  def updateFunc(values: Seq[Int], state: Option[Int]): Option[Int] = {
    Some(values.size + state.getOrElse(0))
  }

  def createFunc(): StreamingContext = {
    val ssc = new StreamingContext(new SparkConf().setAppName("TestUpdateStateByKeyJob"),
      Duration(2000))

    ssc.checkpoint(checkpointDir)

    ssc.socketTextStream("localhost", 9999)
      .flatMap(_.split(" "))
      .map((_, 1))
      .updateStateByKey(updateFunc _)
      .checkpoint(Duration(10000))
      .print()

    ssc
  }
}
