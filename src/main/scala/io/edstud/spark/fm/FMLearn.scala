package io.edstud.spark.fm

import scala.reflect.ClassTag
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import breeze.linalg.SparseVector
import io.edstud.spark.DataSet

abstract class FMLearn (
    val num_factor: Int,
    val num_iter: Int) extends Serializable with Logging  {

    def learn(data: DataSet): FMModel

    protected def logParameter(name: String, value: Double) {
        //logDebug("Updated " + name + " to " + value)
    }

}