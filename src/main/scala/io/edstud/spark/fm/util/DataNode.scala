package io.edstud.spark.fm.util

import scala.collection.Map
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import breeze.linalg.SparseVector
import com.github.nscala_time.time.Imports._

class DataNode protected (nodeType: Int) extends Serializable {

    val isIdentity: Boolean = (nodeType == DataNode.IDENTITY)
    val isList: Boolean = (nodeType == DataNode.LIST)
    val isNumber: Boolean = (nodeType == DataNode.NUMBER)

    val isTarget: Boolean = is("Target")

    def process(value: String): (Long, Long) = transform match {
        case Some(mapping) => (mapping(value), 1)
        case _ => (1, value.toLong)
    }

    private var property: Option[String] = None

    def is(prop2: String): Boolean = property match {
        case Some(prop1) => prop1.toUpperCase.equals(prop2.toUpperCase)
        case _ => false
    }

    private def withProperty(property: String): this.type = {
        this.property = Some(property)

        this
    }

    private var dimension: Long = 0

    def getDimension(): Long = {
        this.dimension
    }

    def withDimension(dimension: Long): this.type = {
        this.dimension = dimension

        this
    }

    private var transform: Option[Map[String, Long]] = None

    def withTransform(transform: Map[String, Long]): this.type = {
        this.transform = Some(transform)

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
        new DataNode(NUMBER)
    }

    def Target(): DataNode = {
        DataNode.Number.withProperty("Target")
    }

    def Time(): DataNode = {
        DataNode.Number.withProperty("Time")
    }

}