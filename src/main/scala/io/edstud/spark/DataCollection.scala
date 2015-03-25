package io.edstud.spark

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel
import breeze.linalg.SparseVector

class DataCollection protected (
    val trainingSet: DataSet,
    val testSet: DataSet,
    val validationSet: DataSet,
    val numFeature: Int = 0) extends Logging with Serializable {

    @transient lazy val dimension = if (numFeature > 0) {
        numFeature
    } else {
        List(
            trainingSet.dimension,
            testSet.dimension,
            validationSet.dimension
        ).max
    }

}

object DataCollection {

    def splitByRandom(
        rawData: RDD[(Double, SparseVector[Double])],
        trainWeight: Double,
        testWeight: Double,
        validateWeight: Double = 0.0): DataCollection = {

        if (trainWeight == 0 || testWeight == 0) {
            throw new Exception("Both TrainingSet and TestSet are required")
        }

        var weights = Array(trainWeight, testWeight)
        if (validateWeight > 0) weights = weights :+ validateWeight

        val data = rawData.randomSplit(weights)
        val collection = new DataCollection(
            DataSet("TrainingSet", data(0)),
            DataSet("TestSet", data(1)),
            DataSet("ValidationSet", if (validateWeight > 0) data(2) else rawData.sparkContext.emptyRDD),
            DataSet.dimension(rawData)
        )

        collection
    }

}