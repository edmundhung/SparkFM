package io.edstud.spark.fm.util

import scala.collection.Map
import scala.collection.immutable.Vector
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import breeze.linalg.SparseVector

class DataNode protected (nodeType: Int) extends Logging with Serializable {

    val isIdentity: Boolean = (nodeType == DataNode.IDENTITY)
    val isList: Boolean = (nodeType == DataNode.LIST)
    val isNumber: Boolean = (nodeType == DataNode.NUMBER)

    def isTarget: Boolean = isTargetNode
    def isInput: Boolean = isInputNode

    private var isInputNode: Boolean = true
    private var isTargetNode: Boolean = false

    def withIsInput(isInputNode: Boolean): this.type = {
        this.isInputNode = isInputNode

        this
    }

    def withIsTarget(isTargetNode: Boolean): this.type = {
        this.isTargetNode = isTargetNode

        this
    }

    def preprocess(feature: String): SparseVector[Double] = {
        val vector = SparseVector.zeros[Double](this.dimension.toInt)

        if (isList) {
            val features = feature.split(this.getSeperator)
            index(features).zip(transform(features)).foreach {
                case (index, value) => vector.update(index, value)
            }
        } else {
            vector.update(index(feature), transform(feature))
        }

        vector
    }

    private def index(features: Array[String]): Array[Int] = {
        features.map(index)
    }

    private def index(feature: String): Int = indexer match {
        case Some(indexing) => indexing(feature).toInt
        case _ => 0
    }

    private def transform(features: Array[String]): Array[Double] = transformer match {
        case Some(transformer) =>
            transformer.transform(features)
        case _ =>
            distribution(features)
    }

    private def distribution(features: Array[String]): Array[Double] = {
        Array.fill[Double](features.size)(1.0 / features.size)
    }

    private def transform(feature: String): Double = transformer match {
        case Some(transformer) => transformer.transform(feature)
        case _ => 1
    }

    private var dimension: Long = 0

    def getDimension(): Long = {
        this.dimension
    }

    def withDimension(dimension: Long): this.type = {
        this.dimension = dimension

        this
    }

    private var seperator: Option[String] = None

    def withSeperator(symbol: String): this.type = {
        this.seperator = Some(symbol)

        this
    }

    def getSeperator(): String = {
        """\Q%s\E""".format(this.seperator.getOrElse(","))
    }

    private var indexer: Option[Map[String, Long]] = None

    def withIndexer(indexer: Map[String, Long]): this.type = {
        this.indexer = Some(indexer)

        this
    }

    private var transformer: Option[DataTransformer] = None

    def withTransformer(transformer: DataTransformer): this.type = {
        this.transformer = Some(transformer)

        this
    }

}

object DataNode {

    private val IDENTITY = 1
    private val LIST = 2
    private val NUMBER = 3

    def Identity(): DataNode = {
        new DataNode(IDENTITY)
    }

    def List(): DataNode = {
        new DataNode(LIST)
    }

    def Number(): DataNode = {
        new DataNode(NUMBER).withTransformer(BypassData)
    }

    def Target(): DataNode = {
        DataNode.Number.withIsTarget(true).withIsInput(false)
    }

    def Bypass(): DataNode = {
        DataNode.Number.withIsInput(false)
    }

    def Time(): DataNode = {
        DataNode.Number.withTransformer(TimeStampToMonths)
    }

}
