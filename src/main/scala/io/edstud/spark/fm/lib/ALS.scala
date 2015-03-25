package io.edstud.spark.fm.lib

import scala.collection.mutable.Map
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import breeze.linalg.{SparseVector, DenseVector, DenseMatrix}
import io.edstud.spark.fm.bs._
import io.edstud.spark.fm.{FMModel,FMLearn}
import io.edstud.spark.DataSet

class ALS protected () extends FMLearn {

    override def learn(fm: FMModel, dataset: DataSet): FMModel = {

        logDebug("Precomputing e(x,y|theta)...")
        var error = precomputeTermE(fm, dataset)

        if (fm.k0) {

            logDebug("Start learning w0")
            var w0 = fm.w0

            fm.w0 = drawGlobalBias(fm.w0, fm.reg0, dataset.size, error)

            if (w0 != fm.w0) {
                error = error.map(e => e + (fm.w0 - w0))
            }

        }

        var e = tranformAsMap(error.zipWithIndex.mapValues(_.toInt).map(_.swap))

        logDebug("Precomputing Dataset transpose...")
        val features = dataset.transposeInput.collectAsMap

        if (fm.k1) {

            logDebug("Start learning W")
            var w = fm.w.copy

            for (id <- 0 until fm.num_attribute) {

                if (features.contains(id)) {

                    fm.w(id) = drawTheta(fm.w(id), fm.regw, features(id), e)

                    if (w(id) != fm.w(id)) {
                        e = updateTermE(e, fm.w(id) - w(id), features(id))
                    }

                }

            }
        }

        logDebug("Start learning V")
        val v = fm.v.copy

        for (f <- 0 until fm.num_factor) {

            logDebug("Precomputing q(x,f|theta) for f = " + (f + 1)+ "...")
            var q = tranformAsMap(precomputeTermQ(fm, dataset, f))

            for (id <- 0 until fm.num_attribute) {

                if (features.contains(id) && q.contains(id)) {

                    var h = features(id).mapActivePairs( (index, value) => value * q(id) - value * value * fm.v(f, id) )
                    fm.v(f, id) = drawTheta(fm.v(f, id), fm.regv, h, e)

                    if (v(f, id) != fm.v(f, id)) {
                        var diff = fm.v(f, id) - v(f, id)
                        e = updateTermE(e, diff, h)
                        q(id) = updateTermQ(q(id), diff, features(id))
                    }

                }

            }

        }

        fm
    }

/*
    override def learn(fm: FMModel, dataset: RelationalData): FMModel = {

        logDebug("Precomputing e(x,y|theta)...")
        var error = precomputeTermE(fm, dataset)

        if (fm.k0) {

            logDebug("Start learning w0")
            var w0 = fm.w0

            fm.w0 = drawGlobalBias(fm.w0, fm.reg0, dataset.size, error)

            if (w0 != fm.w0) {
                error = error.map(e => e + (fm.w0 - w0))
            }

        }

        var e = error
        val meta = dataset.meta

        if (fm.k1) {

            logDebug("Start learning W")

            var attrOffset = 0

            for (relation <- dataset.relations) {

                val rowMapping = dataset.inputs.map(_.valueAt(relation.mappingOffset)).cache()

                rowMapping.zip(e).groupByKey.mapValues(_.sum).foreach { case (rid, total) =>
                    relation.cache(rid.toInt).we = total
                }

                val cacheMapping = rowMapping.map(rid => relation.cache(rid.toInt)).cache()

                e = e.zip(cacheMapping).map { case (e, cache) => e - cache.y }

                relation.transpose.map { case (index, features) =>
                    val id = index + attrOffset
                    val g = meta.attrGroup(id)
                    fm.w(id) = drawRelationTheta(fm.w(id), fm.regw, features, relation.cache)
                }

                e = e.zip(cacheMapping).map { case (e, cache) => e + cache.y }

            }
        }


        fm
    }
*/

    protected def tranformAsMap(rdd: RDD[(Int, Double)]): Map[Int, Double] = {
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

    private def drawGlobalBias(theta: Double, reg: Double, size: Int, error: RDD[Double]): Double = {
        val theta_new = (error.reduce(_ + _) - theta * size) / (reg + size)

        if (isUpdatable(theta_new, theta)) {
            theta_new
        } else {
            theta
        }
    }

    private def drawTheta(theta: Double, reg: Double, h: SparseVector[Double], e: Map[Int, Double]): Double = {
        val (sum_h_sqr, sum_e_h) = computeThetaComponents(h, e)
        val theta_new: Double = - (sum_e_h - theta * sum_h_sqr) / (reg + sum_h_sqr)

        if (isUpdatable(theta_new, theta)) {
            theta_new
        } else {
            theta
        }

    }

    private def drawRelationTheta(theta: Double, reg: Double, h: SparseVector[Double], cache: Array[RelationCache]): Double = {

        theta
    }

    protected def computeThetaComponents(h: SparseVector[Double], e: Map[Int, Double]): (Double, Double) = {
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

    protected def updateTermE(e: Map[Int, Double], thetaDiff: Double, h: SparseVector[Double]): Map[Int, Double] = {
        h.activeIterator.foreach { case (index, value) =>
            e(index) += value * thetaDiff
        }

        e
    }

    protected def updateTermQ(q: Double, thetaDiff: Double, h: SparseVector[Double]): Double = {
        q + h.activeValuesIterator.reduce(_ + _) * thetaDiff
    }

}

object ALS {

    def run(): ALS = {
        new ALS()
    }

}