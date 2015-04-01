package io.edstud.spark.fm.util

import scala.collection.Map
import scala.collection.mutable.{Map => MutableMap}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import breeze.linalg.SparseVector

class RelationVectorizor() extends StandardVectorizor {

    // Method 1: Materialize all relation <- Current method
    // Method 2: Progressive materialize relationship

    private var relations = MutableMap[Int, Map[String, Array[((Long, Long), Long)]]]()

    def addRelation(relation: RDD[Array[String]], definition: Map[Int, DataNode], mapping: Int): this.type = {

        logInfo("Processing new relation...")

        val (identities, inputs) = identify(relation, definition)
        val stats = super.analyzeFeatures(inputs, definition)
        val transform = super.preprocess(inputs, stats)
        val relationMap = identities.zip(transform).collectAsMap

        relations(mapping) = relationMap

        this
    }

    override protected def preprocess(inputs: RDD[Array[String]], stats: Map[Int, DataNode]): RDD[Array[((Long, Long), Long)]] = {

        logInfo("Preprocessing transformation...")

        inputs.map { features =>
            features.zipWithIndex.flatMap { case (feature, index) =>
                val node = stats(index)

                if (relations.contains(index)) {
                    relations(index)(feature)
                } else {
                    Array((node.process(feature), node.getDimension))
                }
            }
        }

    }

}