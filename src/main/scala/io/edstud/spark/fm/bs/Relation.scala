package io.edstud.spark.fm.bs

import org.apache.spark.rdd.RDD
import breeze.linalg.SparseVector
import io.edstud.spark.fm.bs._
import io.edstud.spark.Features

class Relation (features: RDD[SparseVector[Double]]) extends Features(features) {

    var isTemporary: Boolean = false;
    val meta: Metadata = new Metadata(dimension)

    def setTemporary(): this.type = {
        isTemporary = true

        this
    }

}