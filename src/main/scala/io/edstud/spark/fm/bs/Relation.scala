package io.edstud.spark.fm.bs

import org.apache.spark.rdd.RDD
import breeze.linalg.SparseVector
import io.edstud.spark.fm.bs._
import io.edstud.spark.Features

class Relation (features: RDD[SparseVector[Double]], offset: Int) extends Features(features) {

    var isTemporary: Boolean = false
    val meta: Metadata = new Metadata(dimension)
    val cache: Array[RelationCache] = null
    val mappingOffset: Int = offset

    def setTemporary(): this.type = {
        isTemporary = true

        this
    }

}

class RelationCache () {
    var wnum: Double = 0      // #
	var q: Double = 0      // q_if^B
	var wc: Double = 0      // c_if^B
	var wc_sqr: Double = 0  // c_if^B,S
	var y: Double = 0      // y_i^B
	var we: Double = 0      // e_i
	var weq: Double = 0     // e_if^B,q
}