package io.edstud.spark.fm.lib

import scala.collection.mutable.Map
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import breeze.linalg.{SparseVector, DenseVector, DenseMatrix}
import io.edstud.spark.fm.bs._
import io.edstud.spark.fm.{FMModel,FMLearn}
import io.edstud.spark.DataSet

class ALS protected () extends FMLearn {

    var e: Map[Int, Double] = null
    var q: Map[Int, Double] = null

    //val emptyRow = SparseVector.zeros[Double](1)

    override def learn(fm: FMModel, dataset: DataSet): FMModel = {

        logDebug("Precomputing e(x,y|theta)...")
        e = asMap(precomputeTermE(fm, dataset).zipWithIndex.mapValues(_.toInt).map(_.swap))

        logDebug("Precomputing Dataset transpose...")
        val features = dataset.transposeInput.collectAsMap

        if (fm.k0) {
            logDebug("Start learning w0")
            fm.w0 = drawTheta(fm.w0, SparseVector.fill[Double](dataset.size)(1), fm.reg0)
        }

        if (fm.k1) {
            logDebug("Start learning W")
            for (id <- 0 until fm.num_attribute) {
                if (features.contains(id)) {
                    fm.w(id) = drawTheta(fm.w(id), features(id), fm.regw)
                }
            }
        }

        logDebug("Start learning V")
        val v = fm.v.copy
        for (f <- 0 until fm.num_factor) {

            logDebug("Precomputing q(x,f|theta) for f = " + (f + 1)+ "...")
            q = asMap(precomputeTermQ(fm, dataset, f))

            for (id <- 0 until fm.num_attribute) {

                if (features.contains(id) && q.contains(id)) {

                    v(f, id) = drawTheta(fm.v(f, id), features(id).mapActivePairs { case (index, value) =>
                        value * q(id) - value * value * fm.v(f, id)
                    }, fm.regv)

                    features(id).activeIterator.foreach { case (index, value) =>
                        q(id) += value * (v(f, id) - fm.v(f, id))
                    }

                    fm.v(f, id) = v(f, id)

                }

            }

        }

        fm
    }

/*
    override def learn(fm: FMModel, dataset: RelationalData): FMModel = {

        fm
    }
*/

    protected def asMap(rdd: RDD[(Int, Double)]): Map[Int, Double] = {
        Map[Int, Double](rdd.collectAsMap.toSeq: _*)
    }

    protected def precomputeTermE(fm: FMModel, dataset: DataSet): RDD[Double] = {
        dataset.rdd.mapValues(fm.predict).map(r => r._2 - r._1)
    }

    protected def precomputeTermQ(fm: FMModel, dataset: DataSet, f: Int): RDD[(Int, Double)] = {
        dataset.transposeInput.map { case (id, features) =>
            val sum = features.activeIterator.map(pair => fm.v(f, id) * pair._2).reduce(_+_)

            (id, sum)
        }
    }

    private def drawTheta(theta: Double, h: SparseVector[Double], reg: Double): Double = {
        val (sum_h_sqr, sum_e_h) = computeThetaComponents(h)
        val theta_new: Double = - (sum_e_h - theta * sum_h_sqr) / (reg + sum_h_sqr)
        val isThetaUpdatable = isUpdatable(theta_new, theta)

        if (isThetaUpdatable) {
            updateError(theta_new - theta , h)
        }

        if (isThetaUpdatable) {
            theta_new
        } else {
            theta
        }

    }

    protected def computeThetaComponents(h: SparseVector[Double]): (Double, Double) = {
        var sum_h_sqr: Double = 0
        var sum_e_h: Double = 0

        if (h.used > 0) {
            sum_h_sqr = h.activeIterator.map(pair => pair._2 * pair._2).reduce(_ + _)
            sum_e_h = h.activeIterator.map(pair => e(pair._1) * pair._2).reduce(_ + _)
        }

        (sum_h_sqr, sum_e_h)
    }

    protected def isUpdatable(newValue: Double, value: Double): Boolean = {
        !newValue.isNaN && !newValue.isInfinite && newValue != value
    }

    protected def updateError(thetaDiff: Double, h: SparseVector[Double]) {
        h.activeIterator.foreach { pair =>
            e(pair._1) += pair._2 * thetaDiff
        }
    }

}

object ALS {

    def run(): ALS = {
        new ALS()
    }

}