package io.edstud.spark.fm.bs

import org.apache.spark.rdd.RDD
import breeze.linalg.{SparseVector, DenseVector}
import io.edstud.spark.DataSet

class Metadata (
    val numRelations: Int,
    val numAttrGroups: Int,
    val dimension: Int) {

    val attrGroup: DenseVector[Int] = DenseVector.zeros[Int](dimension)
    val numAttrPerGroup: DenseVector[Int] = DenseVector.fill[Int](numAttrGroups)(dimension)
}
