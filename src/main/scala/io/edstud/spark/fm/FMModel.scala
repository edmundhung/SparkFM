package io.edstud.spark.fm

import org.apache.spark.Logging
import breeze.linalg._
import breeze.stats.distributions.Gaussian
import io.edstud.spark.Model

class FMModel(
    val num_attribute: Int = 0,
    val num_factor: Int = 0,
    val	init_mean: Double = 0.0,
    val init_stdev: Double = 0.01) extends Model with Serializable {

    // Sampling
    val gaussian = new Gaussian(init_mean, init_stdev)

    // Model parameters
    var w0: Double = 0
    val w: DenseVector[Double] = DenseVector.zeros[Double](num_attribute)
    val v: DenseMatrix[Double] = DenseMatrix.zeros[Double](num_factor, num_attribute).map(value => gaussian.draw)

    // Bias control
    val k0: Boolean = true
    val k1: Boolean = true

    // Regularization parameters
    var reg0: Double = 0.0
    var regw: Double = 0.0
    var regv: Double = 0.0

    // TODO: Optimize factors calculation
    override def predict(features: SparseVector[Double]): Double = {
        var result: Double = 0;

        if (k0) {
		    result += w0;
	    }
        if (k1) {
            result += sum(w :* features)
    	    }
        if (num_factor > 0) {
            val m = v.mapPairs( (index, value) => features(index._2) * value )
            val m_sqr = m.mapValues(d => d * d)
            val m_sum = sum(m(*, ::))
            val m_sqr_sum = sum(m_sqr(*, ::))
            result += 0.5 * sum(m_sum.mapValues(d => d * d) - m_sqr_sum)
        }

        result
    }
}