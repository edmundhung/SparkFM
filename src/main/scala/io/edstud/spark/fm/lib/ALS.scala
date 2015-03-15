package io.edstud.spark.fm.lib

import scala.collection.mutable.Map
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import breeze.linalg.{SparseVector, DenseVector, DenseMatrix, sum}
import io.edstud.spark.fm.{FMModel,FMLearn}
import io.edstud.spark.DataSet

class ALS protected () extends FMLearn {

    var e: Map[Long, Double] = null
    var q: Map[Long, Double] = null

    protected def precomputeTermE(fm: FMModel, dataset: DataSet): Map[Long, Double] = {
        val map = dataset.rdd
                    .mapValues(fm.predict)
                    .map(r => r._1 - r._2)
                    .zipWithIndex
                    .map(_.swap)
                    .collectAsMap

        Map[Long, Double](map.toSeq: _*)
    }

    protected def precomputeTermQ(fm: FMModel, dataset: DataSet, f: Int): Map[Long, Double] = {
        val dimension = dataset.dimension
        val map = dataset.inputs.map { input =>
            input.activeIterator.filter(_._1 < dimension).map(pair => fm.v(f, pair._1) * pair._2).reduce(_+_)
        }.zipWithIndex.map(_.swap).collectAsMap

        Map[Long, Double](map.toSeq: _*)
    }

    override def learn(fm: FMModel, dataset: DataSet): FMModel = {

        logDebug("Precomputing e(x,y|theta)...")
        e = precomputeTermE(fm, dataset)

        logDebug("Precomputing Dataset transpose...")
        var data_t = dataset.transposeInput.zipWithIndex.map(_.swap).collectAsMap

        if (fm.k0) {
            logDebug("Start learning w0")
            fm.w0 = drawTheta(fm.w0, SparseVector.fill[Double](dataset.size)(1), fm.reg0)
        }

        if (fm.k1) {
            logDebug("Start learning W")
            for (id <- 0 until fm.num_attribute) {
                fm.w(id) = drawTheta(fm.w(id), data_t(id), fm.regw)
            }

        }

        logDebug("Start learning V")
        val v = fm.v.copy
        for (f <- 0 until fm.num_factor) {

            logDebug("Precomputing q(x,f|theta) for f = " + (f + 1)+ "...")
            q = precomputeTermQ(fm, dataset, f)

            for (id <- 0 until fm.num_attribute) {

                v(f, id) = drawTheta(fm.v(f, id), data_t(id).mapActivePairs { case (index, value) =>
                    value * q(index) - value * value * fm.v(f, id)
                }, fm.regv)

                data_t(id).activeIterator.foreach { case (index, value) =>
                    q(index) -= value * (fm.v(f, id) - v(f, id))
                }

                fm.v(f, id) = v(f, id)
            }

        }

        fm
    }

    protected def drawTheta(theta: Double, h: SparseVector[Double], reg: Double): Double = {

        val sum_h_sqr: Double = h.activeIterator.map(pair => pair._2 * pair._2).reduce(_+_)
        val sum_e_h: Double = h.activeIterator.map(pair => e(pair._1) * pair._2).reduce(_+_)
        var theta_new: Double = (sum_e_h + sum_h_sqr * theta) / (sum_h_sqr + reg)

        if (theta.isNaN || theta.isInfinite) {
            theta_new = theta
        }

        val theta_diff = theta - theta_new

        if (theta_diff > 0) {
            h.activeIterator.foreach { pair =>
                e(pair._1) -= pair._2 * theta_diff
            }
        }

        theta_new
    }

}

object ALS {

    def run(): ALS = {
        new ALS()
    }

}