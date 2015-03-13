package io.edstud.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel
import breeze.linalg.SparseVector

class DataSet(protected val dataset: RDD[(Double, SparseVector[Double])] = null) extends Serializable {

    val isEmpty: Boolean = rdd == null

    def rdd: RDD[(Double, SparseVector[Double])] = {
        if (isEmpty) throw new Exception()

        dataset
    }

    def inputs: RDD[SparseVector[Double]] = {
        if (isEmpty) throw new Exception()

        dataset.map(data => data._2)
    }

    def targets: RDD[Double] = {
        if (isEmpty) throw new Exception()

        dataset.map(data => data._1)
    }

    //@transient lazy val isEmpty: Boolean = rdd.partitions.length == 0 || rdd.take(1).length == 0

    // toInt: Assuming the datasets are less then 2,147,483,647
    @transient lazy val size: Int = {
        if (!isEmpty) {
            dataset.count.toInt
        } else {
            0
        }
    }

    @transient lazy val dimension: Int = {
        if (!isEmpty) {
            inputs.map(_.index.lastOption.getOrElse(0)).reduce(math.max) + 1
        } else {
            0
        }
    }

    val transposeInput: RDD[SparseVector[Double]] = {
        if (isEmpty) throw new Exception()

        val count = size
        inputs.zipWithIndex.flatMap { case (input, index) =>
            input.activeKeysIterator.zip(input.activeValuesIterator.map((index.toInt,_)))
        }.groupByKey.map(_._2.unzip).map { case (indices, values) =>
            new SparseVector(indices.toArray, values.toArray, count)
        }
    }

    def cache(): DataSet = {
        if (!isEmpty) dataset.cache()

        this
    }

    def unpersist(): DataSet = {
        if (!isEmpty) dataset.unpersist()

        this

    }

}

object DataSet {

    def apply(rdd: RDD[(Double, SparseVector[Double])]): DataSet = {
        new DataSet(rdd)
    }

    def empty(): DataSet = {
        new DataSet()
    }

    def dimension(rdd: RDD[(Double, SparseVector[Double])]): Int = {
        new DataSet(rdd).dimension
    }

}