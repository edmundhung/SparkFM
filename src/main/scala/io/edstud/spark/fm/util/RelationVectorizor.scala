package io.edstud.spark.fm.util

import scala.collection.Map
import scala.collection.mutable.{Map => MutableMap}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import breeze.linalg.SparseVector

class RelationVectorizor() extends StandardVectorizor {

    // Method 1: Materialize all relation <- Current method
    // Method 2: Progressive materialize relationship

    private var relations = MutableMap[Int, Map[String, Array[SparseVector[Double]]]]()
    private var dimensions = 0

    def addRelation(relation: RDD[Array[String]], definition: Map[Int, DataNode], mapping: Int): this.type = {

        logInfo("Processing new relation...")

        val (targets, inputs) = identify(relation, definition)
        val stats = super.analyzeFeatures(inputs, definition)
        val transform = super.preprocess(inputs, stats)
        dimensions += super.computeDimension(stats)

        relations(mapping) = targets.zip(transform).collectAsMap

        this
    }

    override protected def preprocess(inputs: RDD[Array[String]], nodes: Map[Int, DataNode]): RDD[Array[SparseVector[Double]]] = {
        inputs.map { features =>
            features.zipWithIndex.flatMap {
                case (feature, index) =>
                    if (relations.contains(index) && relations(index).contains(feature)) {
                        relations(index)(feature)
                    } else {
                        Array(nodes(index).preprocess(feature))
                    }
            }
        }
    }

    override protected def computeDimension(nodes: Map[Int, DataNode]): Int = {
        super.computeDimension(nodes) + dimensions - relations.size
    }

}