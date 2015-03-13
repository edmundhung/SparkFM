package io.edstud.spark

import scala.reflect.ClassTag
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import breeze.linalg.SparseVector

abstract class Model () extends Serializable with Logging {

    def predict(features: SparseVector[Double]): Double

    def computeRMSE(dataset: DataSet): Double = {
        logDebug("Start computing RMSE...")
        val rmse = math.sqrt(dataset.rdd.mapValues(predict).map(r => r._1 - r._2).map(e => math.pow(e, 2)).mean())
        logDebug("Result RMSE = " + rmse)

        rmse
    }

    def computeMAE(dataset: DataSet): Double = {
        logDebug("Start computing MAE...")
        val mae = dataset.rdd.mapValues(predict).map(r => r._1 - r._2).map(e => math.abs(e)).mean()
        logDebug("Result MAE = " + mae)

        mae
    }

    def evaluateRegression(dataset: DataSet): (Double, Double) = {
        (computeRMSE(dataset), computeMAE(dataset))
    }

    def evaluateClassification(dataset: DataSet): Double = {
        dataset.rdd.mapValues(predict).filter(r => (r._1 >= 0 && r._2 >= 0) || (r._1 < 0 && r._2 < 0)).count / dataset.rdd.count
    }

}