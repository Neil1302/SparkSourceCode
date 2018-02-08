package examples.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc

/**
 * Porting /src/main/python/sql/parquet/parquet_test.py
 * to Scala
 */
case class Purchase(date: String, time: String,
	city: String, category: String, amount: Double, method: String)

object TestParquet {
	def main(args: Array[String]): Unit = {

		if (args.length == 0) {
			println("Error: Please mention the parquet file...")
			sys.exit(-1)
		}

		val spark = SparkSession
			.builder()
			.master("local[4]")
			.appName("TestParquet")
			.config(conf = new SparkConf())
			.getOrCreate()

		import spark.implicits._

		val ds = spark.read.parquet(args(0)).as[Purchase]

		ds.printSchema()

		println("Number of Method=\"Discover\": "
			+ ds.filter(data => data.method == "Discover").count())

		// Top 3 methods...
		println(">>> Top 3 Methods using Dataframe API >>>")

		ds.groupBy("Method")
			.sum("Amount")
			.withColumnRenamed("sum(Amount)", "Total")
			.orderBy(desc("Total"))
			.take(3)
			.foreach(println)

		println(">>> Top 3 Methods using SQL >>>")

		ds.createOrReplaceTempView("temp")
		val sqlStr = "select Method, sum(Amount) as Total from temp group by Method order by Total desc"

		spark
			.sql(sqlStr)
			.take(3)
			.foreach(println)
	}
}