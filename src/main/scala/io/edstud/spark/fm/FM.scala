package io.edstud.spark.fm

import scala.language.implicitConversions
import scala.reflect.ClassTag

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

import io.edstud.spark.DataSet
import io.edstud.spark.Task._
import io.edstud.spark.fm.impl._
import io.edstud.spark.fm.bs.Relation
import io.edstud.spark.fm.util._

abstract class FM protected () extends Serializable with Logging {

    def withVectorizer(vectorizer: StandardVectorizer): this.type

    def withRelation(relation: Relation): this.type

    def learnWith(fml: FMLearn): FMModel

}

object FM {

    def apply(
        dataset: DataSet,
        numFactor: Int,
        task: Task = Regression,
        maxIteration: Int = 100,
        timeout: Int = 0): FM = {

        new FactorizationMachines(dataset, numFactor, task, maxIteration, timeout)
    }

}