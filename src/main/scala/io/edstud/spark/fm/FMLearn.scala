package io.edstud.spark.fm

import scala.reflect.ClassTag
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import breeze.linalg.SparseVector
import io.edstud.spark.DataSet

abstract class FMLearn () extends Serializable with Logging  {

    def learn(fm: FMModel, dataset: DataSet): FMModel

}