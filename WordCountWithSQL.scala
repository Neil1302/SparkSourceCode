package examples.streaming

import org.apache.spark.streaming.{ StreamingContext, Seconds }
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.SQLContext

/**
 * Alternative to DStream.reduceByKeyAndWindow using DataFrames.
 * Listens to socket text stream on host=localhost, port=9999.
 * Tokenizes the incoming stream into (words, no. of occurrences).
 * */
object WordCountWithSQL {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("StreamingWithSQL")

    val ssc = new StreamingContext(conf, Seconds(10))
    // create an sql context for SparkSQL operations
    val sqlContext = new SQLContext(ssc.sparkContext)

    /**
     * This is required for implicit conversions of
     *  RDD to DataFrame.
     */
    import sqlContext.implicits._

    ssc
      .socketTextStream("localhost", 9999)
      .flatMap(_.split(" "))
      .map((_, 1))
      .foreachRDD { rdd =>
        /**
         * This piece of code should be executed for each RDD to get a DF from it
         */
        val df = rdd.toDF("word", "count")
        df.registerTempTable("word_count")
        
        df.groupBy("word").count.show

      }

    ssc.start()
    ssc.awaitTermination()
  }
}