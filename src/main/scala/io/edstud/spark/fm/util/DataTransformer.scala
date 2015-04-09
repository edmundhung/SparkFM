package io.edstud.spark.fm.util

trait DataTransformer extends Serializable {
    def transform(feature: String): Double
    def transform(features: Array[String]): Array[Double] = features.map(transform)
}

object BypassData extends DataTransformer {
    def transform(feature: String): Double = feature.toDouble
}

object TimeStampToMonths extends DataTransformer {
    def transform(feature: String): Double = feature.toLong / (60 * 60 * 24 * 7 * 30)
}
