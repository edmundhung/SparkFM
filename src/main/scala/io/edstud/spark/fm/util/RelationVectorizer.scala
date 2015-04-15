package io.edstud.spark.fm.util

import scala.collection.Map
import scala.collection.mutable.{Map => MutableMap}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import breeze.linalg.SparseVector

class RelationVectorizer() extends StandardVectorizer {

    // Method 1: Materialize all relation <- Current method
    // Method 2: Progressive materialize relationship

    private var relations = MutableMap[Int, Map[String, Array[SparseVector[Double]]]]()
    private var dimensions = 0

    def addRelation(relationRDD: RDD[Array[String]], relationDomains: Map[Int, DataDomain], mapping: Int): this.type = {

        logDebug("Processing Relation (name = %s)".format(relationRDD.name))

        val (identityRDD, featuresRDD) = identify(relationRDD, relationDomains)
        val featureDomains = super.analyzeFeatures(featuresRDD, relationDomains)
        val formatted = super.format(featuresRDD, featureDomains)

        dimensions += super.computeDimension(featureDomains)

        relations(mapping) = identityRDD.zip(formatted).collectAsMap

        this
    }

    override protected def format(featuresRDD: RDD[Array[String]], domains: Map[Int, DataDomain]): RDD[Array[SparseVector[Double]]] = {
        featuresRDD.map { features =>
            features.zipWithIndex.flatMap {
                case (feature, index) =>
                    var features = Array[SparseVector[Double]]() :+ domains(index).format(feature)

                    if (relations.contains(index)) {
                        if (relations(index).contains(feature))
                            features = features ++ relations(index)(feature) // Append relation vector
                        else
                            features = Array[SparseVector[Double]]() // No mapping exists, filter it
                    }

                    features
            }
        }
    }

    override protected def computeDimension(domains: Map[Int, DataDomain]): Int = {
        super.computeDimension(domains) + dimensions// - relations.size
    }

}