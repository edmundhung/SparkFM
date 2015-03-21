package io.edstud.spark.fm.impl

import scala.collection.immutable.List
import io.edstud.spark.DataSet
import io.edstud.spark.fm._
import io.edstud.spark.fm.bs._
import io.edstud.spark.Task._

class FactorizationMachines (
    val dataset: DataSet,
    val numFactor: Int = 8,
    val task: Task = Regression,
    val maxIteration: Int = 100,
    val timeout: Int = 0) extends FM {

    var relations: List[Relation] = List[Relation]()

    def withRelation(relation: Relation): this.type = {
        relations = relations :+ relation

        this
    }

    def learnWith(fml: FMLearn): FMModel = {

        dataset.cache()

        logInfo("Initializing Metadata...")
        val meta = initMetadata()

        logInfo("Initializing FM Model...")
        var fm = new FMModel(dataset.dimension, numFactor)

        logInfo("Starting Learning Process...")
        for (i <- 1 to maxIteration) {
            logInfo("Iteration " + i + " in progress...")
            fm = fml.learn(fm, dataset, meta)
        }

        dataset.unpersist()

        fm
    }

    private def initMetadata(): Metadata = {
        val numRelations = relations.size
        val numAttrGroups = if (numRelations > 0) relations.map(relation => relation.meta.numAttrGroups).reduce(_+_) else 0
        val meta = new Metadata(numRelations, numAttrGroups, dataset.size)

        meta
    }


}