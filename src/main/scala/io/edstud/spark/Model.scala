package io.edstud.spark

import scala.reflect.ClassTag
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import breeze.linalg.SparseVector

abstract class Model () extends Serializable with Logging {

    def predict(features: SparseVector[Double]): Double

    def computeRMSE(dataset: DataSet): Double = {
        val rmse_sqr = dataset.rdd.mapValues(predict).map(r => r._1 - r._2).map(e => e * e).sum()
        val rmse = math.sqrt(rmse_sqr / dataset.size)
        logInfo(dataset.rdd.name + " RMSE = " + rmse)

        rmse
    }

    def computeMAE(dataset: DataSet): Double = {
        val mae = dataset.rdd.mapValues(predict).map(r => r._1 - r._2).sum() / dataset.size
        logInfo(dataset.rdd.name + " MAE = " + mae)

        mae
    }

    def computeAccuracy(dataset: DataSet): Double = {
        dataset.rdd.mapValues(predict).filter(r => (r._1 >= 0 && r._2 >= 0) || (r._1 < 0 && r._2 < 0)).count / dataset.size
    }

}