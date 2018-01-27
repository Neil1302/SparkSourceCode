
package examples.graphx

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.{ Edge, Graph }

/**
 * Employee relationship analysis with GraphX.
 */
object EmployeeRelationship {
	def main(args: Array[String]): Unit = {
		// vertex format: vertex_id, data
		val vertexArray = Array(
			(1L, ("John", "Software Developer")),
			(2L, ("Robert", "Technical Leader")),
			(3L, ("Charlie", "Software Architect")),
			(4L, ("David", "Software Developer")),
			(5L, ("Edward", "Software Development Manager")),
			(6L, ("Francesca", "Software Development Manager")))

		// edge format: from_vertex_id, to_vertex_id, data
		val edgeArray = Array(
			Edge(2L, 1L, "Technical Mentor"),
			Edge(2L, 4L, "Technical Mentor"),
			Edge(3L, 2L, "Collaborator"),
			Edge(6L, 3L, "Team Member"),
			Edge(4L, 1L, "Peers"),
			Edge(5L, 2L, "Team Member"),
			Edge(5L, 3L, "Team Member"),
			Edge(5L, 6L, "Peers"))

		val sc = new SparkContext(new SparkConf().setAppName("EmployeeRelationshipJob"))

		val vertexRDD: RDD[(Long, (String, String))] = sc.parallelize(vertexArray)

		val edgeRDD: RDD[Edge[String]] = sc.parallelize(edgeArray)

		val graph: Graph[(String, String), String] = Graph(vertexRDD, edgeRDD)

		// Vanilla query
		println(">>> Showing the names of people who are Software Developers")
		graph.vertices.filter { case (id, (name, designation)) => designation.equals("Software Developer") }
			.collect()
			.foreach { case (id, (name, designation)) => println(s"... Name: $name, Designation: $designation") }

		// Connection analysis
		println(">>> People connected to Robert (Technical Leader) -> ")
		graph.triplets.filter(_.srcId == 2).collect()
			.foreach { item => println("... " + item.dstAttr._1 + ", " + item.dstAttr._2) }

		println(">>> Robert (Technical Leader) connected to -> ")
		graph.triplets.filter(_.dstId == 2).collect()
			.foreach { item => println("... " + item.srcAttr._1 + ", " + item.srcAttr._2) }

		println(">>> Technical Mentoring Analysis -> ")
		graph.triplets.filter(_.attr.equals("Technical Mentor")).collect()
			.foreach { item => println("... " + item.srcAttr._1 + " mentoring " + item.dstAttr._1) }
	}
}