package com.kafka.spark.prwa

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka
import org.apache.spark.streaming.kafka.KafkaUtils


object TestKafka {
  def main(args: Array[String]) {
    
    val dp = new SparkConf().setAppName("kafka and spark integration")
    .setMaster("local[*]")
    val ssc = new StreamingContext(dp, Seconds(10))
    
    val lines = KafkaUtils.createStream(ssc, "localhost:2181", "spark_group_kafka", Map("customer" -> 3))
    lines.print()
    ssc.start()
    ssc.awaitTermination()
  }
  
}