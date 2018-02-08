package examples

import org.apache.spark.{ SparkConf, SparkContext, HashPartitioner }
import org.apache.spark.rdd.PairRDDFunctions

case class Customer(ID: Int, name: String)
case class Item(ID: Int, name: String, price: Float)
case class Order(ID: Int, item: Item, quantity: Int, var discount: Float)
case class CustomerOrders(cust: Customer, order: Order, offer: Boolean)

object TestValueTransformations {
	def main(args: Array[String]): Unit = {
		val sc = new SparkContext(new SparkConf().setAppName("TestCombineByKeyJob"))
		val rdd = sc.parallelize(
			List(
				CustomerOrders(Customer(1, "A"), Order(1, Item(1, "item_1", 20), 2, 0), false),
				CustomerOrders(Customer(1, "A"), Order(2, Item(2, "item_2", 10), 1, 0), false),
				CustomerOrders(Customer(2, "B"), Order(1, Item(1, "item_1", 20), 2, 0), true)))

		println(">>> List of customers availing offers")
		/**
		 * Career limiting code, only for educational use...
		 */
		rdd.foreach {
			(custOrder: CustomerOrders) =>
				if (custOrder.offer)
					println(custOrder.toString())
		}
		// Exercise: Get an RDD of customers availing offers?

		// Insert discounts of 20% for offers
		val ordersAfterDiscountsPerCustomer = rdd
			.map((custOrders: CustomerOrders) => (custOrders.cust.ID, custOrders))
			// for every key pass the value through a mapping function
			.mapValues { custOrders =>
				if (custOrders.offer)
					custOrders.order.discount = 0.2f

				custOrders
			}

		/**
		 * Career limiting code, only for educational use to see the contents of the RDD...
		 */
		println(">>> List of customers availing offers along with discounts")
		ordersAfterDiscountsPerCustomer.foreach { item =>
			println("...Customer ID: " + item._1.toString())
			println(item._2.toString())
		}

		val orderValuePerCustomer = ordersAfterDiscountsPerCustomer
			.flatMapValues { custOrders =>
				Seq(custOrders.order.item.price * custOrders.order.quantity * (1 - custOrders.order.discount))
			}

		/**
		 * Career limiting code, only for educational use to see the contents of the RDD...
		 */
		orderValuePerCustomer.foreach(println)

		println(">>> Total order value for customer ID = 1 is " + orderValuePerCustomer.reduceByKey(_ + _).lookup(1).toString())

	}
}