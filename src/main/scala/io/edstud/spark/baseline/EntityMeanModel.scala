package io.edstud.spark.baseline

import scala.collection.Map
import breeze.linalg.SparseVector
import io.edstud.spark.Model

class EntityMeanModel (value: Map[Int, Double]) extends Model with Serializable {

    private val UserMean: Map[Int, Double] = value

    override def predict(features: SparseVector[Double]): Double = {
        features.activeKeysIterator.filter {
            id => UserMean.contains(id)
        }.map {
            id => UserMean(id)
        }.reduce(_+_)
    }

}