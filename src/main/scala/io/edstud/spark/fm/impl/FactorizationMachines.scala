package io.edstud.spark.fm.impl

import scala.collection.immutable.List
import org.apache.spark.storage.StorageLevel
import io.edstud.spark.DataSet
import io.edstud.spark.fm._
import io.edstud.spark.fm.bs._
import io.edstud.spark.Task._

class FactorizationMachines (
    protected var dataset: DataSet,
    protected val numFactor: Int = 8,
    protected val task: Task = Regression,
    protected val maxIteration: Int = 100,
    protected val timeout: Int = 0) extends FM {

    protected var relations: List[Relation] = List[Relation]()

    def withRelation(relation: Relation): this.type = {
        relations = relations :+ relation

        this
    }

    def withRelation(relationSet: List[Relation]): this.type = {
        relations = relations ::: relationSet

        this
    }

    def learnWith(fml: FMLearn): FMModel = {

        if (!relations.isEmpty) {
            dataset = RelationalData(dataset.rdd, relations)
        }

        dataset.persist(StorageLevel.MEMORY_AND_DISK_SER)

        logInfo("Initializing FM Model...")
        var fm = new FMModel(dataset.dimension, numFactor)

        logInfo("Starting Learning Process...")
        for (i <- 1 to maxIteration) {
            fm.computeRMSE(dataset)
            logInfo("Iteration " + i + " in progress...")
            fm = fml.learn(fm, dataset)
        }

        dataset.unpersist()

        fm
    }


}