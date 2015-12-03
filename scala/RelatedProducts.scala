/**
 * This file contains scala code to represent product dataset in form of a graph
 * (viz. products representing vertices and edges representing products bought together)
 * to compute disjoint sets of replated products using Apache Spark Graphx library. 
 */

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.spark._
import org.apache.spark.SparkContext._

@SerialVersionUID(21312L)
class Product(val asin: String, val category: String,
	      val component: Long) extends Serializable {
	
	override def toString(): String = 
		     { component + "-" + asin + "," + category }
}

@SerialVersionUID(23424L)
class DisjointSets(data: RDD[String]) extends Serializable {

	private final val RECORD_FIELDS: Int = 3
	private final val UNKNOWN_CATEGORY: String = "other"

	private var productGraph: Graph[Product,Int] = null

	private def vertexHashId(asin: String): VertexId = {
	 asin.toLowerCase.hashCode.toLong
	}

	private def isEmpty(asin: String): Boolean = {
	 asin == null || asin.trim.length == 0
	}

	private def isValidRecord(record: String): Boolean = {
	 !isEmpty(record) && record.split("\\|").length == RECORD_FIELDS
	}

	def createGraph() {
		val splitRecord = data.filter(record => isValidRecord(record))
				      .map(record => record.split("\\|"))

		val vertex1 = splitRecord.filter(record => (!isEmpty(record(0).trim)))
			                 .map(record => (vertexHashId(record(0)),
							 new Product(record(0), record(2), 0)))

		val vertex2 = splitRecord.filter(record => !isEmpty(record(1).trim))
			      .flatMap{ record => (record(1).split("\\s*,\\s*")
					.map(v => (vertexHashId(v),
						   new Product(v, UNKNOWN_CATEGORY, 0)))) }

		val vertex = vertex1.union(vertex2)

		val edge = splitRecord.filter(record => !isEmpty(record(1).trim))
				      .flatMap{ record => (record(1).split("\\s*,\\s*")
						.map(v => Edge(vertexHashId(v),
							       vertexHashId(record(0)), 1))) }

		productGraph = Graph(vertex, edge)
	}

	def categorizeProducts(): RDD[Product] = {

		productGraph.joinVertices(productGraph.connectedComponents.vertices) 
			{ case (id, p, leader) => new Product(p.asin, p.category, leader) }
		
		// we just need products
		.vertices.map{ case(id, product) => product }
	}
}

object RelatedProducts {

	def main(args: Array[String]) {
		

		if (null == args || args.length < 2) {
			println("Usage: SomeJar.jar <input-path> <output-path>")
			return 
		} 

		val conf = new SparkConf().setAppName("RelatedProducts")
		val sc = new SparkContext(conf)
		
		val data: RDD[String] = sc.textFile(args(0))
		val disjointSets = new DisjointSets(data)
		disjointSets.createGraph()
		
		val categorizedProducts: RDD[Product] = disjointSets.categorizeProducts()

		categorizedProducts.saveAsTextFile(args(1))
		
		sc.stop()
	}
}

