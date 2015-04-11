package io.edstud.spark.dbscan

import scala.collection.JavaConverters._
import scala.collection.Map
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import breeze.linalg.SparseVector
import org.apache.commons.math3.ml.distance.{DistanceMeasure, EuclideanDistance, ManhattanDistance}
import org.apache.commons.math3.ml.clustering.{DoublePoint, DBSCANClusterer, Cluster}
import com.github.nscala_time.time.Imports._

class DBSCAN protected (dataset: RDD[(Int, Array[(Double, Double)])]) {

    def learnWith(eps: Double, minPts: Int, distanceMeasure: DistanceMeasure): DBSCANModel = {

        val clustersByUser = dataset.mapValues {
            locationsByUser => DBSCAN.cluster(locationsByUser, eps, minPts, distanceMeasure)
        }

        new DBSCANModel(clustersByUser)
    }

    def learnWith(eps: Double, minPts: Int): DBSCANModel = learnWith(eps, minPts, new EuclideanDistance ())

}

object DBSCAN {

    def apply(checkinsByUserId: RDD[(Int, Array[(Int, DateTime)])], locationsByVenueId: Map[Int, (Double, Double)]): DBSCAN = {
        new DBSCAN(checkinsByUserId.mapValues { venueIdsWithDateTime =>
            venueIdsWithDateTime.map { case (id, datetime) =>
                locationsByVenueId(id)
            }
        })
    }

    def byWorkingArea(checkinsByUserId: RDD[(Int, Array[(Int, DateTime)])], locationsByVenueId: Map[Int, (Double, Double)]): DBSCAN = {
        DBSCAN(checkinsByUserId.mapValues { venueIdsWithDateTime =>
            venueIdsWithDateTime.filter { case (id, datetime) =>
                val isWeekday = datetime.getDayOfWeek match {
                    case x if (x >= 1 && x <= 5) => true
                    case _ => false
                }

                val isOfficeHour = datetime.getHourOfDay match {
                    case x if (x >= 8 && x <= 18) => true
                    case _ => false
                }

                isWeekday && isOfficeHour
            }
        }, locationsByVenueId)
    }

    protected def retrieveClusters(locations: Array[(Double, Double)], eps: Double, minPts: Int, distanceMeasure: DistanceMeasure): Array[Cluster[DoublePoint]] = {
        val dbscan: DBSCANClusterer[DoublePoint] = new DBSCANClusterer[DoublePoint](eps, minPts, distanceMeasure)

        val clusters: Array[Cluster[DoublePoint]] = dbscan.cluster {
            locations.map {
                points =>
                    val d = Array[Double](points._1, points._2)
                    new DoublePoint(d)
            }.toSeq.asJava
        }.asScala.toArray

        clusters
    }

    protected def cluster(locations: Array[(Double, Double)], eps: Double, minPts: Int, distanceMeasure: DistanceMeasure): Map[Int, Array[(Double, Double)]] = {

        var minPoints = minPts
        var clusters: Array[Cluster[DoublePoint]] = Array[Cluster[DoublePoint]]()

        do {
            clusters = retrieveClusters(locations, eps, minPoints, distanceMeasure)
            minPoints = minPoints - 1
        } while (clusters.size == 0 && minPoints > 0)

        val c = if (clusters.size > 0) {
            clusters.map { cluster =>
                cluster.getPoints.asScala.toArray.map { point =>
                    val p = point.getPoint

                    (p(0), p(1))
                }
            }
        } else {
            locations.map {
                point => Array[(Double, Double)](point)
            }
        }

        c.zipWithIndex.map(_.swap).toMap
    }

}