package io.edstud.spark.baseline

import breeze.linalg.SparseVector
import io.edstud.spark.Model

class GlobalMeanModel (value: Double) extends Model with Serializable {

    private val GlobalMean: Double = value

    override def predict(features: SparseVector[Double]): Double = {
        GlobalMean
    }

}