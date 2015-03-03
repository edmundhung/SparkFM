package io.edstud.spark.fm

import scala.language.implicitConversions
import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import io.edstud.spark.DataSet
import io.edstud.spark.Task._
import io.edstud.spark.fm.impl._

abstract class FM protected () extends Serializable {

    def withRelation(): this.type
    def learnWith(fml: FMLearn): FMModel

}

object FM {

    def apply(dataset: DataSet, task: Task): FM = {
        new FactorizationMachines(dataset, task)
    }

}