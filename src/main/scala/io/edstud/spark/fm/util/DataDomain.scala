package io.edstud.spark.fm.util

import scala.collection.Map
import scala.collection.immutable.Vector
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import breeze.linalg.SparseVector

class DataDomain protected (domain: Int) extends Logging with Serializable {

    val isCategory: Boolean = domain == DataDomain.CATEGORY
    val isSet: Boolean = domain == DataDomain.SET
    val isRealValue: Boolean = domain == DataDomain.REALVALUE
    def isTarget: Boolean = (domain == DataDomain.RATING) || (domain == DataDomain.IDENTITY)
    def isRequired: Boolean = domain != DataDomain.RATING && domain != DataDomain.NONE

    def format(feature: String): SparseVector[Double] = {
        val vector = SparseVector.zeros[Double](this.dimension)

        domain match {
            case DataDomain.SET =>
                val features = feature.split(this.delimiter)
                val values = Array.fill[Double](features.size)(1.0d / features.size)
                features.map(index).zip(values).foreach {
                    case (index, value) => vector.update(index, value)
                }
            case DataDomain.CATEGORY =>
                vector.update(index(feature), 1)
            case DataDomain.REALVALUE =>
                vector.update(0, preprocess(feature) match {
                    case f: Double => f
                    case _ => feature.toDouble
                })
        }

        vector
    }

    private def index(feature: String): Int = indexer match {
        case Some(indexing) => indexing(feature).toInt
        case _ => 0
    }

    private def preprocess(feature: Any): Any = {
        preprocessors.foldLeft(feature)((value, preprocessor) => preprocessor.preprocess(value))
    }

    def dimension(): Int = indexer match {
        case Some(indexing) => indexing.size
        case _ => 1
    }

    private var name: Option[String] = None

    def withName(name: String): this.type = {
        this.name = Some(name)

        this
    }

    def attributeName: String = name.getOrElse("")

    private var delimiterSymbol: Option[String] = None

    protected def withDelimiter(symbol: String): this.type = {
        this.delimiterSymbol = Some(symbol)

        this
    }

    def delimiter(): String = """\Q%s\E""".format(
        this.delimiterSymbol.getOrElse(",")
    )

    private var indexer: Option[Map[String, Long]] = None

    def withIndexer(indexer: Map[String, Long]): this.type = {
        this.indexer = Some(indexer)

        this
    }

    def getAllIndex(): Array[String] = indexer match {
        case Some(indexing) => indexing.keys.toArray
        case _ => Array()
    }

    private var preprocessors: Array[DataPreprocessor] = Array[DataPreprocessor]()

    def withPreprocessor(preprocessor: DataPreprocessor): this.type = {
        preprocessors = preprocessors :+ preprocessor

        this
    }

}

object DataDomain {

    private val NONE = 0
    private val CATEGORY = 1
    private val SET = 2
    private val REALVALUE = 3
    private val RATING = 4
    private val IDENTITY = 5

    def Categorical(): DataDomain = {
        new DataDomain(CATEGORY)
    }

    def CategoricalSet(delimiter: String): DataDomain = {
        new DataDomain(SET).withDelimiter(delimiter)
    }

    def RealValued(): DataDomain = {
        new DataDomain(REALVALUE)
    }

    def Rating(): DataDomain = {
        new DataDomain(RATING)
    }

    def Bypass(): DataDomain = {
        new DataDomain(NONE)
    }

    def Identity(): DataDomain = {
        new DataDomain(IDENTITY)
    }

}
