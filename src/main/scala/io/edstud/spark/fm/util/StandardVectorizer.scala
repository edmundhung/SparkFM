package io.edstud.spark.fm.util

import scala.collection.Map
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkContext._
import breeze.linalg.SparseVector

class StandardVectorizer() extends Logging with Serializable {

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

    protected def format(inputs: RDD[Array[String]], domains: Map[Int, DataDomain]): RDD[Array[SparseVector[Double]]] = {
        inputs.map { features =>
            features.zipWithIndex.map {
                case (feature, index) => domains(index).format(feature)
            }
        }
    }

    protected def computeDimension(domains: Map[Int, DataDomain]): Int = {
        domains.filter(_._2.isRequired).map(_._2.dimension).reduce(_ + _)
    }

    def transform(rawRDD: RDD[Array[String]], rawDomains: Map[Int, DataDomain]): RDD[(Double, SparseVector[Double])] = {
        logDebug("Identifying Data Structure...")
        val (ratingRDD, featuresRDD) = identify(rawRDD, rawDomains)

        logDebug("Analyzing features distribution...")
        val featureDomains = analyzeFeatures(featuresRDD, rawDomains)
        val dimension = computeDimension(featureDomains)

        logDebug("Start vectorization (Dimension: %d)".format(dimension))
        val featureVectorRDD = format(featuresRDD, featureDomains).map { array =>
            val f = SparseVector.zeros[Double](dimension + 1)

            var offset = 0
            array.foreach { features =>
                features.activeIterator.foreach { pair =>
                    f.update(pair._1 + offset, pair._2)
                }
                offset += features.size
            }

            f
        }

        ratingRDD.map(_.toDouble).zip(featureVectorRDD)
    }

}