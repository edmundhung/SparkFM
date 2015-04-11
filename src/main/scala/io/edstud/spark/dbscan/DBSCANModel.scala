package io.edstud.spark.dbscan

import scala.collection.Map
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

class DBSCANModel (clustersByUserRDD: RDD[(Int, Map[Int, Array[(Double, Double)]])]) extends Serializable {

    private val clustersByUser: Map[Int, Map[Int, Array[(Double, Double)]]] = clustersByUserRDD.collectAsMap

    def getClutsersByUserId(userId: Int): Map[Int, Array[(Double, Double)]] = {
        clustersByUser(userId)
    }

    def info() {
        println("---------------------------------------------- ")
        println("Total count: " + clustersByUserRDD.cache.count)
        println("Count with cluster(s): " + clustersByUserRDD.filter(_._2.size > 0).count)
        println("Count with at least 2 clusters: " + clustersByUserRDD.filter(_._2.size >= 2).count)
        println("---------------------------------------------- ")
    }

}