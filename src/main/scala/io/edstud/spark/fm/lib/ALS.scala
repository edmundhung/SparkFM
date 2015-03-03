package io.edstud.spark.fm.lib

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import breeze.linalg.{Vector, DenseVector, DenseMatrix, sum}
import io.edstud.spark.fm.{FMModel,FMLearn}
import io.edstud.spark.DataSet

class ALS protected (num_factor: Int, num_iter: Int) extends FMLearn (num_factor, num_iter) {

    var error_train: RDD[Double] = null
    var error_test: RDD[Double] = null

    def learnRegression(dataset: DataSet): FMModel = {
        _learn(dataset)
    }

    def learnClassification(dataset: DataSet): FMModel = {
        _learn(dataset)
    }

    protected def _learn(dataset: DataSet): FMModel = {

        var error_train: RDD[Double] = null
        var error_test: RDD[Double] = null

        var fm = new FMModel(dataset.dimension, num_factor)

        for (i <- 0 until num_iter) {
            fm = draw(fm, dataset)
        }

        fm
    }

    protected def precomputeTermE(fm: FMModel, dataset: DataSet): RDD[Double] = {
        dataset.rdd.mapValues(fm.predict).map(r => r._2 - r._1)
    }

    protected def precomputeTermQ(fm: FMModel, dataset: DataSet, f: Int): RDD[Double] = {
        dataset.inputs.map(input => sum(input.mapPairs( (i, value) => fm.v(f, i) * value)))
    }

    protected def draw(fm: FMModel, dataset: DataSet): FMModel = {
        var error = precomputeTermE(fm, dataset)
        var data_t = dataset.transpose.zipWithIndex.cache()
        if (fm.k0) {
            var h0 = DenseVector.ones[Double](dataset.size)
            var w0 = drawTheta(fm.w0, error, h0, fm.reg0)
            if(w0 != fm.w0) {
                error = update(error, h0, fm.w0 - w0)
                fm.w0 = w0
            }
        }
        if (fm.k1) {
            var w = fm.w.copy
            data_t.foreach {
                case (features, index) => {
                    var id = index.toInt
                    w(id) = drawTheta(fm.w(id), error, features, fm.regw)
                    if(w(id) != fm.w(id)) {
                        error = update(error, features, fm.w(id) - w(id))
                        fm.w(id) = w(id)
                    }
                }
            }

        }
        if (fm.num_factor > 0) {
            var v = fm.v.copy
            for (f <- 0 until fm.num_factor) {
                var q = precomputeTermQ(fm, dataset, f).zipWithIndex.map(_.swap).collectAsMap

                data_t.foreach {
                    case (features, index) => {
                        var id = index.toInt
                        var h = features.mapPairs {
                            case (index, value) => value * q(index) - value * value * v(f, index)
                        }
                        v(f, id) = drawTheta(fm.v(f, id), error, h, fm.regv)
                        if (v(f, id) != fm.v(f, id)) {
                            error = update(error, h, fm.v(f, id) - v(f, id))
                            fm.v(f, id) = v(f, id)
                        }
                    }
                }
            }
        }

        fm
    }

    protected def drawTheta(theta: Double, error: RDD[Double], h: Vector[Double], reg: Double): Double = {

        var sum_h_sqr: Double = sum(h :* h)
        var sum_e_h: Double = error.zipWithIndex.mapValues(id => h(id.toInt)).map(v => v._1 * v._2).reduce(_+_)
        var sum_h_theta: Double = sum(h :* theta)

        var theta_new: Double = - 1.0 * (sum_e_h - sum_h_theta) / (sum_h_sqr + reg)

        if (!theta.isNaN && !theta.isInfinite) {
            theta_new = theta
        }

        theta_new
    }

    protected def update(error: RDD[Double], h: Vector[Double], thetaDiff: Double): RDD[Double] = {
        error.zipWithIndex.mapValues(id => h(id.toInt)).map(v => v._1 - v._2 * thetaDiff)
    }


}

object ALS {

    def run(num_factor: Int, num_iter: Int): ALS = {
        val als = new ALS(num_factor, num_iter)

        als
    }

}