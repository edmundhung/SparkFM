package io.edstud.spark.fm.impl

import java.util.Date
import io.edstud.spark.DataSet
import io.edstud.spark.fm._
import io.edstud.spark.Task._

class FactorizationMachines (
    val dataset: DataSet,
    val numFactor: Int = 8,
    val task: Task = Regression,
    val maxIteration: Int = 100,
    val timeout: Int = 0) extends FM {

    def within(minutes: Int): this.type = {
        this
    }

    def withRelation(): this.type = {
        this
    }

    def learnWith(fml: FMLearn): FMModel = {
        dataset.cache()

        logInfo("Initializing FM Model...")
        var fm = new FMModel(dataset.dimension, numFactor)

        logInfo("Starting Learning Process...")
        for (i <- 0 until maxIteration) {
            logInfo("Iteration " + (i + 1) + " in progress...")
            fm = fml.learn(fm, dataset)
        }

        dataset.unpersist()

        fm
    }

}