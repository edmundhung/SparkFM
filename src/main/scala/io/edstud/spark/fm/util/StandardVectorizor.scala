package io.edstud.spark.fm.util

import scala.collection.Map
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import breeze.linalg.SparseVector

class StandardVectorizor() extends Logging {

    protected def identify(raw: RDD[Array[String]], definition: Map[Int, DataNode]): (RDD[String], RDD[Array[String]]) = {

        logInfo("Identifying Data Structure...")

        val dataset = raw.filter(features => features.size == definition.size).cache()

        val targets = dataset.map {
            features => features.zipWithIndex.filter(f => definition(f._2).isTarget).map(_._1).head
        }
        val inputs = dataset.map {
            features => features.zipWithIndex.filter(f => !definition(f._2).isTarget).map(_._1)
        }

        dataset.unpersist()

        (targets, inputs.cache)
    }

    protected def analyzeFeatures(inputs: RDD[Array[String]], definition: Map[Int, DataNode]): Map[Int, DataNode] = {

        logInfo("Analyzing features distribution...")

        var stats = definition.map { case (index, node) =>

            /* TODO - node.isList */

            val statNode = if (node.isIdentity) {
                val feature = inputs.groupBy(features => features(index)).map(_._1).cache()

                val featureMap = feature.zipWithIndex.collectAsMap
                val dimension = feature.count

                feature.unpersist()

                node.withTransform(featureMap).withDimension(dimension)
            } else {
                /* if (node.isNumber) */
                node.withDimension(1)
            }

            (index, statNode)
        }

        stats
    }

    protected def preprocess(inputs: RDD[Array[String]], stats: Map[Int, DataNode]): RDD[Array[((Long, Long), Long)]] = {

        logInfo("Preprocessing transformation...")

        inputs.map { features =>
            features.zipWithIndex.map { case (feature, index) =>
                val node = stats(index)

                (node.process(feature), node.getDimension)
            }
        }
    }

    def transform(raw: RDD[Array[String]], definition: Map[Int, DataNode]): RDD[(Double, SparseVector[Double])] = {
        val (targets, inputs) = identify(raw, definition)
        val stats = analyzeFeatures(inputs, definition)
        val transform = preprocess(inputs, stats)

        logInfo("Transformation in progress...")

        val dimension = stats.map(_._2.getDimension).reduce(_ + _)
        val featureVectors = transform.map { featureStats =>
            val f = SparseVector.zeros[Double](dimension.toInt)

            var offset = 0
            featureStats.foreach { case ((index, value), featureDimension) =>
                f.update(index.toInt + offset, value)
                offset += featureDimension.toInt
            }

            f
        }

        targets.map(_.toDouble).zip(featureVectors)
    }

}