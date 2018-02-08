package examples.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

// Model for people.json
case class People(name: String, age: Option[Long])
// Model for users.parquet
case class Users(name: String, favorite_color: Option[String], favorite_numbers: Option[Array[Int]])

/**
 * Test class for Spark Dataset.
 * Handles null values in JSON/Parquet files.
 * @author prithvi
 */
object TestDataset {
	def main(args: Array[String]): Unit = {

		val spark = SparkSession.builder()
			.master("local[4]")
			.appName("TestDataset")
			.config(conf = new SparkConf())
			.getOrCreate()

		readJson(spark)

		readParquet(spark)

	}

	def readParquet(spark: SparkSession): Unit = {
		import spark.implicits._
		val parquetFile = "/media/linux-1/spark-2.0.0-bin-hadoop2.7/examples/src/main/resources/users.parquet"
		val usersDS = spark.read.parquet(parquetFile).as[Users]

		usersDS.printSchema()

		usersDS.show()

	}

	def readJson(spark: SparkSession): Unit = {
		import spark.implicits._
		val jsonFile = "/media/linux-1/spark-2.0.0-bin-hadoop2.7/examples/src/main/resources/people.json"
		val peopleDS = spark.read.json(jsonFile).as[People]

		val partialFilterAge = filterAge(20, _: People)
		peopleDS.filter(partialFilterAge).show()
	}

	def filterAge(condition: Long, data: People): Boolean = {
		val ret: Boolean = data.age match {
			case Some(value) =>
				if (value > condition) true
				else false
			case None => false
		}
		ret
	}
}