package examples

import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast

import scala.io.Source
import scala.util.{ Try, Success, Failure }
import scala.collection.mutable.Map

/**
 * Test code for broadcast variables.
 * For more info read,
 * http://spark.apache.org/docs/latest/programming-guide.html#broadcast-variables.
 *
 * Here a map of countries along with their capitals are kept as a
 * broadcast variable.
 * This variable is used for arbitrary lookup for capitals of a country.
 *
 */
object TestBroadcastVariables {
	def main(args: Array[String]): Unit = {

		loadCSVFile("/media/linux-1/spark-dev/data/countries.csv") match {
			case Some(countries) => {
				val sc = new SparkContext(new SparkConf()
					.setAppName("TestBroadcastVariablesJob"))

				val countriesCache = sc.broadcast(countries)

				val countriesRDD = sc.parallelize(countries.keys.toList)

				// happy case...
				val happyCaseRDD = searchCountryDetails(countriesRDD, countriesCache, "A")
				println(">>>> Search results of countries starting with 'A': " + happyCaseRDD.count())
				happyCaseRDD.foreach(entry => println("Country:" + entry._1 + ", Capital:" + entry._2))

				// sad case...
				val sadCaseRDD = searchCountryDetails(countriesRDD, countriesCache, "Zz")
				println(">>>> Search results of countries starting with 'Zz': " + sadCaseRDD.count())
				sadCaseRDD.foreach(entry => println("Country:" + entry._1 + ", Capital:" + entry._2))
			}
			case None => println("Error loading file...")
		}

	}

	/**
	 * Filters the input countries' RDD based on the search token and then 
	 * extracts their corresponding capitals from the broadcast variable.
	 * Subsequently the searched countries and capitals are stored in a paired RDD 
	 * and returned to the caller.
	 */
	def searchCountryDetails(countriesRDD: RDD[String], countryCache: Broadcast[Map[String, String]],
		searchToken: String): RDD[(String, String)] = {
		countriesRDD.filter(_.startsWith(searchToken))
			.map(country => (country, countryCache.value(country)))
	}

	/**
	 * Loads a CSV file from disk.
	 * Returns a map as (key=country, value=capital).
	 * Returns Some(map) on Success or None on Failure.
	 */
	def loadCSVFile(filename: String): Option[Map[String, String]] = {
		val countries = Map[String, String]()

		Try {
			val bufferedSource = Source.fromFile(filename)

			for (line <- bufferedSource.getLines) {
				val Array(country, capital) = line.split(",").map(_.trim)
				countries += country -> capital
			}

			bufferedSource.close()
			return Some(countries)

		}.toOption
	}
}
