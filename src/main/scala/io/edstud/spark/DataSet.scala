package io.edstud.spark

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel
import breeze.linalg.SparseVector

class Features(protected val features: RDD[SparseVector[Double]]) extends Logging with Serializable {

    lazy val isEmpty = checkEmpty(features)

    lazy val transpose = transposeRDD(features, size)

    lazy val size = computeSize(features)

    lazy val dimension = computeDimension(features)

    protected def checkEmpty(rdd: RDD[SparseVector[Double]]): Boolean = {
        rdd.partitions.length == 0 || rdd.take(1).length == 0
    }

    protected def computeSize(rdd: RDD[SparseVector[Double]]): Int = {
        if (!isEmpty) rdd.count.toInt else 0
    }

    protected def computeDimension(rdd: RDD[SparseVector[Double]]): Int = {
        if (!isEmpty) rdd.map(_.index.max).reduce(math.max) else 0
    }

    protected def transposeRDD(rdd: RDD[SparseVector[Double]], size: Int = 0): RDD[(Int, SparseVector[Double])] = {
        val count = if (size > 0) { size } else { rdd.count.toInt }
        rdd.zipWithIndex.flatMap { case (input, index) =>
            input.activeKeysIterator.zip(input.activeValuesIterator.map((index.toInt,_)))
        }.groupByKey.mapValues(_.unzip).mapValues { case (indices, values) =>
            new SparseVector(indices.toArray, values.toArray, count)
        }.setName("%s.transpose".format(rdd.name))
    }

}

class DataSet(val rdd: RDD[(Double, SparseVector[Double])]) extends Features(rdd.map(_._2)) {

    val inputs = features.setName("%s.inputs".format(rdd.name))

    val targets = rdd.map(_._1).setName("%s.targets".format(rdd.name))

    lazy val transposeInput = transpose.cache

    def cache(): this.type = {
        if (!isEmpty) rdd.cache()

        this
    }

    def persist(level: StorageLevel): this.type = {
        if (!isEmpty) rdd.persist(level)

        this
    }

    def unpersist(): this.type = {
        if (!isEmpty) rdd.unpersist()

        this
    }

}

object DataSet {

    def apply(name: String, rdd: RDD[(Double, SparseVector[Double])]): DataSet = {
        new DataSet(rdd.setName(name))
    }

    def dimension(rdd: RDD[(Double, SparseVector[Double])]): Int = {
        new DataSet(rdd).size
    }

}