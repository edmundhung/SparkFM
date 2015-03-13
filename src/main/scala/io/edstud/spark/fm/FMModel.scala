package io.edstud.spark.fm

import scala.util.Random
import org.apache.spark.Logging
import breeze.linalg._
import breeze.stats.distributions.Gaussian
import io.edstud.spark.Model

class FMModel(
    val num_attribute: Int,
    val num_factor: Int,
    val init_mean: Double = 0,
    val init_stdev: Double = 0.01,
    val seed: Long = 0) extends Model with Serializable {

    // Model parameters
    var w0: Double = 0
    val w: DenseVector[Double] = DenseVector.zeros[Double](num_attribute)
    val v: DenseMatrix[Double] = DenseMatrix.zeros[Double](num_factor, num_attribute).mapPairs { case (index, value) =>
        val rng = new Random(index._1 * num_factor + index._2 + seed)
        init_mean + init_stdev * rng.nextGaussian
    }
    // Bias control
    val k0: Boolean = true
    val k1: Boolean = true

    // Regularization parameters
    var reg0: Double = 0.0
    var regw: Double = 0.0
    var regv: Double = 10.0//0.0

    // TODO: Optimize factors calculation
    override def predict(features: SparseVector[Double]): Double = {

        var result: Double = 0;

        if (k0) {
		    result += w0;
	    }

        if (k1) {
            result += features.activeIterator
                        .filter(_._1 < num_attribute)
                        .map(pair => w(pair._1) * pair._2)
                        .reduce(_+_)
    	    }

        for (i <- 0 until num_factor) {
            var f = features.activeIterator
                        .filter(_._1 < num_attribute)
                        .map(pair => v(i, pair._1) * pair._2)
                        .toSet

            val sum_f = f.reduce(_+_)
            val sum_sqr_f = f.map(value => value * value).reduce(_+_)

            result += 0.5 * (sum_f * sum_f - sum_sqr_f)
        }

        result
    }

}