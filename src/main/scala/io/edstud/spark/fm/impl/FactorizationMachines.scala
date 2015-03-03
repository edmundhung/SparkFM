package io.edstud.spark.fm.impl

import io.edstud.spark.DataSet
import io.edstud.spark.fm._
import io.edstud.spark.Task._

class FactorizationMachines (
    val dataset: DataSet,
    val task: Task) extends FM {

    var num_factor: Int = 8

    def withFactorNumber(num: Int): this.type = {
        num_factor = num

        this
    }

    def withRelation(): this.type = {
        this
    }

    def learnWith(fml: FMLearn): FMModel = {
        if (task == Regression) {
            fml.learnRegression(dataset)
        } else if (task == Classification) {
            fml.learnClassification(dataset)
        } else {
            throw new Exception("No such task")
        }
    }

}