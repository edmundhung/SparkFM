package io.edstud.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel
import breeze.linalg.SparseVector

class DataCollection protected (
    val trainingSet: DataSet,
    val testSet: DataSet,
    val validationSet: DataSet,
    val num_feature: Int = 0) extends Serializable {

    @transient lazy val dimension = if (num_feature > 0) {
        num_feature
    } else {
        List(
            trainingSet.dimension,
            testSet.dimension,
            validationSet.dimension
        ).max
    }

}

object DataCollection {

    def byRandomSplit(
        rawData: RDD[(Double, SparseVector[Double])],
        trainWeight: Double,
        testWeight: Double,
        validateWeight: Double = 0.0): DataCollection = {

        if (trainWeight == 0 || testWeight == 0) {
            throw new Exception("Both TrainingSet and TestSet are required")
        }

        val weights = Array(trainWeight, testWeight)
        if (validateWeight > 0) weights :+ validateWeight

        val data = rawData.randomSplit(weights)
        val collection = new DataCollection(
            if (trainWeight > 0) DataSet(data(0)) else DataSet.empty(),
            if (testWeight > 0) DataSet(data(1)) else DataSet.empty(),
            if (validateWeight > 0) DataSet(data(2)) else DataSet.empty(),
            DataSet.dimension(rawData)
        )

        collection
    }

}