package io.edstud.spark.fm

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import breeze.linalg.SparseVector

object FMUtils {
    /**
    * Registers classes that SparkFM uses with Kryo.
    */
    def registerKryoClasses(conf: SparkConf) {
        conf.registerKryoClasses(Array(
                classOf[FMModel],
                classOf[util.DataDomain],
                classOf[util.DataPreprocessor],
                classOf[util.StandardVectorizer],
                classOf[util.RelationVectorizer]
            )
        )
    }

    def loadLibFMFile(sc: SparkContext, path: String, numFeatures: Int, minPartitions: Int): RDD[(Double, SparseVector[Double])] = {
        val parsed = sc.textFile(path, minPartitions)
                        .map(_.trim)
                        .filter(line => !(line.isEmpty || line.startsWith("#")))
                        .map { line =>
                           val items = line.split(' ')
                           val label = items.head.toDouble
                           val (indices, values) = items.tail.filter(_.nonEmpty).map { item =>
                               val indexAndValue = item.split(':')
                               val index = indexAndValue(0).toInt
                               val value = indexAndValue(1).toDouble
                               (index, value)
                           }.unzip
                           (label, indices.toArray, values.toArray)
                        }

        // Determine number of features.
        val d = if (numFeatures > 0) {
            numFeatures
        } else {
            parsed.persist(StorageLevel.MEMORY_ONLY)
            parsed.map { case (label, indices, values) =>
                indices.max
            }.reduce(math.max)
        }

        parsed.map { case (label, indices, values) =>
            (label, new SparseVector(indices, values, d + 1))
        }

    }

    def loadLibFMFile(sc: SparkContext, path: String, numFeatures: Int): RDD[(Double, SparseVector[Double])] = loadLibFMFile(sc, path, numFeatures, sc.defaultMinPartitions)
    def loadLibFMFile(sc: SparkContext, path: String): RDD[(Double, SparseVector[Double])] = loadLibFMFile(sc, path, -1)

    def saveAsLibFMFile(data: RDD[(Double, SparseVector[Double])], dir: String) {
        val dataStr = data.map { case (target, features) =>
            val sb = new StringBuilder(minimizeString(target))
            features.activeIterator.foreach { case (i, v) =>
                sb += ' '
                sb ++= "%d:%s".format(i + 1, minimizeString(v)) //s"${i + 1}:$v"
            }

            sb.mkString
        }
        dataStr.saveAsTextFile(dir)
    }

    private def minimizeString(v: Double): String = {
        val minimized = if (v.isValidInt) "#" else "#.###"
        new java.text.DecimalFormat(minimized).format(v)
    }

}