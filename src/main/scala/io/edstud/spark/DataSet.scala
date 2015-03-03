package io.edstud.spark

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import breeze.linalg.SparseVector

class DataCollection (
    val collection: Array[RDD[(Double, SparseVector[Double])]],
    val num_feature: Int = 0) extends Serializable {

    @transient lazy val trainingSet = new DataSet(collection(0))
    @transient lazy val testSet = new DataSet(collection(1))
    @transient lazy val validationSet = new DataSet(collection(2))

    @transient lazy val dimension = if (num_feature > 0) {
        num_feature
    } else {
        List(trainingSet.dimension, testSet.dimension, validationSet.dimension).max
    }

    def cache(): this.type = {
        collection.foreach(_.cache)

        this
    }

}

object DataCollection {

    def apply(
        rawData: RDD[(Double, SparseVector[Double])],
        train: Double,
        test: Double,
        validate: Double = 0): DataCollection = {

        val splits = rawData.randomSplit(Array(train, test, validate))
        val dimension = rawData.map(data => data._2.activeSize).max()
        val collection = new DataCollection(splits, dimension)

        collection
    }

}

class DataSet(val rdd: RDD[(Double, SparseVector[Double])]) extends Serializable {

    // toInt: Assuming the datasets are less then 2,147,483,647
    @transient lazy val size: Int = rdd.count.toInt

    @transient lazy val dimension: Int = rdd.map(data => data._2.activeSize).max()

    @transient lazy val inputs: RDD[(SparseVector[Double])] = rdd.map(data => data._2)

    @transient lazy val targets: RDD[Double] = rdd.map(data => data._1)

    @transient lazy val transpose: RDD[SparseVector[Double]] = {
        inputs.zipWithIndex.flatMap { case (input, index) => {
                input.activeKeysIterator.zip(input.activeValuesIterator.map((index.toInt,_)))
            }
        }.groupByKey.map { case (id, pairs) => {
                val v = SparseVector.zeros[Double](dimension)
                pairs.foreach(p => v(p._1) = p._2)

                v
            }
        }
    }

}