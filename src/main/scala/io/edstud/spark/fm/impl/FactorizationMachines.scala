package io.edstud.spark.fm.impl

import io.edstud.spark.DataSet
import io.edstud.spark.fm._
import io.edstud.spark.Task._

class FactorizationMachines (
    val dataset: DataSet,
    val task: Task) extends FM {

    def withRelation(): this.type = {
        this
    }

    def learnWith(fml: FMLearn): FMModel = {
        fml.learn(dataset)
        /*
        if (task == Regression) {
            fml.learnRegression(dataset)
        } else if (task == Classification) {
            fml.learnClassification(dataset)
        } else {
            throw new Exception("No such task")
        }
        */
    }

}