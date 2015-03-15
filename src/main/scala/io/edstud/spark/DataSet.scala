package io.edstud.spark

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel
import breeze.linalg.SparseVector

class DataSet(
    protected val name: String = "",
    protected val dataset: RDD[(Double, SparseVector[Double])] = null
    ) extends Logging with Serializable {

    val isEmpty: Boolean = dataset == null

    def rdd: RDD[(Double, SparseVector[Double])] = {
        if (!isEmpty) {
            dataset.setName(name)
        } else {
            throw new Exception("EmptySet")
        }
    }

    def inputs: RDD[SparseVector[Double]] = {
        if (!isEmpty) {
            dataset.map(data => data._2).setName(name + ".inputs")
        } else {
            throw new Exception("EmptySet")
        }
    }

    def targets: RDD[Double] = {
        if (!isEmpty) {
            dataset.map(data => data._1).setName(name + ".targets")
        } else {
            throw new Exception("EmptySet")
        }
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

    def transposeInput: RDD[SparseVector[Double]] = {
        if (!isEmpty) {
            val count = size
            inputs.zipWithIndex.flatMap { case (input, index) =>
                input.activeKeysIterator.zip(input.activeValuesIterator.map((index.toInt,_)))
            }.groupByKey.map(_._2.unzip).map { case (indices, values) =>
                new SparseVector(indices.toArray, values.toArray, count)
            }.setName(name + ".inputs.transpose")
        } else {
            throw new Exception("EmptySet")
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

    def apply(name: String, rdd: RDD[(Double, SparseVector[Double])]): DataSet = {
        new DataSet(name, rdd)
    }

    def empty(name: String): DataSet = {
        new DataSet(name)
    }

    def dimension(rdd: RDD[(Double, SparseVector[Double])]): Int = {
        new DataSet("", rdd).dimension
    }

}