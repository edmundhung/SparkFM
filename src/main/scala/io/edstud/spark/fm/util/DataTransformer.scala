package io.edstud.spark.fm.util

import com.github.nscala_time.time.Imports._

trait DataTransformer extends Serializable {
    def transform(feature: String): Double
    def transform(features: Array[String]): Array[Double] = features.map(transform)
}

object BypassData extends DataTransformer {
    def transform(feature: String): Double = feature.toDouble
}

object IdentityData extends DataTransformer {
    def transform(feature: String): Double = 1.0d
}


object TimeStampToMonths extends DataTransformer {
    def transform(feature: String): Double = feature.toLong / (60 * 60 * 24 * 7 * 30)
}

object TimeStampIsWeekday extends DataTransformer {
    def transform(feature: String): Double = TimeStampParser.getDayOfWeek(feature) match {
        case 1 | 2 | 3 | 4 | 5 => 1.0d
        case 6 | 7 => -1.0d
        case _ => 0d
    }
}

object TimeStampIsWeekend extends DataTransformer {
    def transform(feature: String): Double = TimeStampIsWeekday.transform(feature) * -1d
}

object TimeStampIsAfterOfficeHour extends DataTransformer {
    def transform(feature: String): Double = TimeStampParser.getHourOfDay(feature) match {
        case x if (x >= 18) => 1d
        case y if (y < 18) => -1d
        case _ => 0d
    }
}

object DateTimeIsWeekday extends DataTransformer {
    def transform(feature: String): Double = DateTimeParser.getDayOfWeek(feature) match {
        case 1 | 2 | 3 | 4 | 5 => 1.0d
        case 6 | 7 => -1.0d
        case _ => 0d
    }
}

object DateTimeIsWeekend extends DataTransformer {
    def transform(feature: String): Double = DateTimeIsWeekday.transform(feature) * -1d
}

object DateTimeIsAfterOfficeHour extends DataTransformer {
    def transform(feature: String): Double = DateTimeParser.getHourOfDay(feature) match {
        case x if (x >= 18) => 1d
        case y if (y < 18) => -1d
        case _ => 0d
    }
}

object DateTimeParser {

    def parse(datetime: String) : DateTime = {
        DateTime.parse(datetime, DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"))
    }

    def getDayOfWeek(timestamp: String): Int = {
        this.parse(timestamp).getDayOfWeek
    }

    def getHourOfDay(timestamp: String): Int = {
        this.parse(timestamp).getHourOfDay
    }

}

object TimeStampParser {

    def parse(timestamp: String): DateTime = {
        new DateTime(timestamp.toLong * 1000L)
    }

    def getDayOfWeek(timestamp: String): Int = {
        this.parse(timestamp).getDayOfWeek
    }

    def getHourOfDay(timestamp: String): Int = {
        this.parse(timestamp).getHourOfDay
    }
}