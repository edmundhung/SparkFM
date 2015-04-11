/*
package io.edstud.spark.fm.lib

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import breeze.linalg.{SparseVector, DenseVector, DenseMatrix}
import breeze.stats.distributions.{Gamma, Gaussian}
import io.edstud.spark.fm.bs._
import io.edstud.spark.fm.{FMModel,FMLearn}
import io.edstud.spark.DataSet

class MCMC protected () extends ALS {

    var alpha0: Double = 1.0
    var gamma0: Double = 1.0
    var beta0: Double = 1.0
    var mu0: Double = 0.0

    var alpha: Double = 1.0
    var w0_mean_0: Double = 0.0

    var w_mu: DenseVector[Double] = null
    var w_lambda: DenseVector[Double] = null

    var v_mu: DenseMatrix[Double] = null
    var v_lambda: DenseMatrix[Double] = null

    override def learn(fm: FMModel, dataset: DataSet, relations: List[Relation], meta: Metadata): FMModel = {

        logDebug("Precomputing e(x,y|theta)...")
        val error = precomputeTermE(fm, dataset).cache()
        e = asMap(error)

        logDebug("Drawing Alpha")
        alpha = drawAlpha(alpha, error, dataset.size)

        logDebug("Precomputing Dataset transpose...")
        var data_t = dataset.transposeInput.zipWithIndex.map(_.swap).collectAsMap

        if (fm.k0) {
            logDebug("Start learning w0")
            fm.w0 = drawSampledTheta(fm.w0, SparseVector.fill[Double](dataset.size)(1), w0_mean_0, fm.reg0)
        }

        if (fm.k1) {
            logDebug("Start learning W")
            for (id <- 0 until fm.num_attribute) {
                fm.w(id) = drawSampledTheta(fm.w(id), data_t(id), w_mu(0), w_lambda(0))
            }

        }

        logDebug("Start learning V")
        val v = fm.v.copy
        for (f <- 0 until fm.num_factor) {

            logDebug("Precomputing q(x,f|theta) for f = " + (f + 1)+ "...")
            q = asMap(precomputeTermQ(fm, dataset, f))

            for (id <- 0 until fm.num_attribute) {

                v(f, id) = drawSampledTheta(fm.v(f, id), data_t(id).mapActivePairs { case (index, value) =>
                    value * q(index) - value * value * fm.v(f, id)
                }, v_mu(0, f), v_lambda(0, f))

                data_t(id).activeIterator.foreach { case (index, value) =>
                    q(index) -= value * (fm.v(f, id) - v(f, id))
                }

                fm.v(f, id) = v(f, id)
            }

        }

        fm
    }

    protected def drawSampledTheta(theta: Double, h: SparseVector[Double], mu: Double, lambda: Double): Double = {
        val (sum_h_sqr, sum_e_h) = computeThetaComponents(h)
        val variance = 1.0 / (lambda + alpha * sum_h_sqr)
        val mean = - variance * (alpha * (sum_e_h - theta * sum_h_sqr) - mu * lambda)

        if (isUpdatable(variance, 0)) {
            val gaussian = new Gaussian(mean, math.sqrt(variance))
            val theta_new = gaussian.draw
            val isThetaUpdatable = isUpdatable(theta_new, theta)

            if (isThetaUpdatable) {
                updateError(theta - theta_new, h)
            }

            if (isThetaUpdatable) {
                theta_new
            } else {
                theta
            }

        } else {
            0.0
        }

    }

    protected def drawAlpha(alpha: Double, error: RDD[Double], size: Int): Double = {
        val alphaN = alpha0 + size
        val gammaN = gamma0 + error.map(error => error * error).reduce(_ + _)
        val gamma = new Gamma(alphaN/2, gammaN/2)
        val alpha_new = gamma.draw

        if (isUpdatable(alpha_new, alpha)) {
            alpha_new
        } else {
            alpha
        }
    }

    protected def drawLambda(): Double = {
        1
    }

    protected def drawMu(): Double = {
        1
    }

}
*/

