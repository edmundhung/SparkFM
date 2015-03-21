package io.edstud.spark.fm.bs

import org.apache.spark.rdd.RDD
import breeze.linalg.DenseVector
import io.edstud.spark.DataSet

class Relation (val data: DataSet, val meta: Metadata = null) {
    val mapping: DenseVector[Int] = DenseVector.zeros[Int](data.size)
    val attributeOffset: Int = 0
}