package io.edstud.spark.fm.util

import scala.collection.Map
import scala.collection.mutable.{Map => MutableMap}
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkContext._
import breeze.linalg.SparseVector

class StandardVectorizer() extends Logging with Serializable {

    def domains: Array[DataDomain] = featureDomains.values.toArray
    protected var featureDomains: Map[Int, DataDomain] = Map[Int, DataDomain]()
    protected var dimension: Int = 0

    protected def identify(raw: RDD[Array[String]], domains: Map[Int, DataDomain]): (RDD[String], RDD[Array[String]]) = {
        val dataset = raw.map(features => features.map(_.trim)).filter {
            features => features.filter(_.size > 0).size == domains.size
        }.persist(StorageLevel.MEMORY_ONLY_SER)

        val targetRDD = dataset.map {
            features => features.zipWithIndex.filter(f => domains(f._2).isTarget).map(_._1).head
        }
        val featuresRDD = dataset.map {
            features => features.zipWithIndex.filter(f => domains(f._2).isRequired).map(_._1)
        }

        dataset.unpersist()

        (targetRDD, featuresRDD.cache)
    }

    protected def analyzeFeatures(featuresRDD: RDD[Array[String]], domains: Map[Int, DataDomain]): Map[Int, DataDomain] = {
        domains.values.filter(_.isRequired).zipWithIndex.map(_.swap).toMap.map { case (index, domain) =>

            if (domain.isCategory || domain.isSet) {
                val feature = if (domain.isSet) {
                    featuresRDD.flatMap(features => features(index).split(domain.delimiter))
                } else {
                    featuresRDD.map(features => features(index))
                }

                domain.withIndexer(feature.distinct.zipWithIndex.collectAsMap)
            }

            (index, domain)
        }
    }

    protected def vectorize(inputs: RDD[Array[String]], domains: Map[Int, DataDomain]): RDD[Array[SparseVector[Double]]] = {
        inputs.map(features => vectorize(features, domains))
    }

    protected def vectorize(features: Array[String], domains: Map[Int, DataDomain]): Array[SparseVector[Double]] = {
        features.zipWithIndex.map {
            case (feature, index) => domains(index).format(feature)
        }
    }

    protected def computeDimension(domains: Map[Int, DataDomain]): Int = {
        domains.filter(_._2.isRequired).map(_._2.dimension).reduce(_ + _)
    }

    protected def mergeVectors(vectors: Array[SparseVector[Double]]): SparseVector[Double] = {
        val f = SparseVector.zeros[Double](this.dimension + 1)

        var offset = 0
        vectors.foreach { features =>
            features.activeIterator.foreach { pair =>
                f.update(pair._1 + offset, pair._2)
            }
            offset += features.size
        }

        f
    }

    def transform (rawRDD: RDD[Array[String]], rawDomains: Map[Int, DataDomain]): RDD[(Double, SparseVector[Double])] = {
        logDebug("Identifying Data Structure")
        val (ratingRDD, featuresRDD) = identify(rawRDD, rawDomains)

        logDebug("Analyzing features distribution")
        this.featureDomains = analyzeFeatures(featuresRDD, rawDomains)
        this.dimension = computeDimension(featureDomains)

        logDebug("Start vectorization (Dimension: %d)".format(dimension))
        val featureVectorRDD = vectorize(featuresRDD, featureDomains).map(mergeVectors)

        ratingRDD.map(_.toDouble).zip(featureVectorRDD)
    }

    def fit(params: Map[String, Any]): SparseVector[Double] = {

        val featureVectors = vectorize(
            this.featureDomains.mapValues { domain =>
                params(domain.attributeName).toString
            }.values.toArray,
            this.featureDomains
        )

        mergeVectors(featureVectors)

        /*
        if (this.featureDomains.size != params.size) {
            throw new Exception("Number of parameters do not fit.")
        } else {

        }
        */
    }

}