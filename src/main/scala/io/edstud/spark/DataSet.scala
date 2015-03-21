package io.edstud.spark

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel
import breeze.linalg.SparseVector

class DataSet(
    protected val dataset: RDD[(Double, SparseVector[Double])],
    protected val name: String = "") extends Logging with Serializable {

    val isEmpty: Boolean = dataset.partitions.length == 0 || dataset.take(1).length == 0

    def rdd: RDD[(Double, SparseVector[Double])] = {
        dataset//.setName(name)
    }

    def inputs: RDD[SparseVector[Double]] = {
        dataset.map(data => data._2)//.setName(name + ".inputs")
    }

    def targets: RDD[Double] = {
        dataset.map(data => data._1)//.setName(name + ".targets")
    }

    // toInt: Assuming the datasets are less then 2,147,483,647
    @transient lazy val size: Int = {
        if (!isEmpty) dataset.count.toInt else 0
    }

    @transient lazy val dimension: Int = {
        if (!isEmpty) inputs.map(_.index.max).reduce(math.max) else 0
    }

    def transposeInput: RDD[SparseVector[Double]] = {
        val count = size
        inputs.zipWithIndex.flatMap { case (input, index) =>
            input.activeKeysIterator.zip(input.activeValuesIterator.map((index.toInt,_)))
        }.groupByKey.map(_._2.unzip).map { case (indices, values) =>
            new SparseVector(indices.toArray, values.toArray, count)
        }//.setName(name + ".inputs.transpose")
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
        new DataSet(rdd, name)
    }

    def dimension(rdd: RDD[(Double, SparseVector[Double])]): Int = {
        new DataSet(rdd).dimension
    }

}