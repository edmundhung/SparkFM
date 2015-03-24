package io.edstud.spark.fm

import scala.collection.immutable.List
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import breeze.linalg.SparseVector
import io.edstud.spark.fm.bs._
import io.edstud.spark.DataSet

abstract class FMLearn () extends Serializable with Logging  {

    def learn(fm: FMModel, dataset: DataSet): FMModel

    def learn(fm: FMModel, dataset: RelationalData): FMModel = learn(fm, dataset)

}