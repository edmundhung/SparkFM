package io.edstud.spark.fm.util

import scala.collection.Map
import com.github.nscala_time.time.Imports._
import io.edstud.spark.dbscan._

trait DataPreprocessor extends Serializable {
    def preprocess(data: Any): Any
}

object TimeStampToMonth extends DataPreprocessor {
    override def preprocess(feature: Any): Double = feature match {
        case timestamp: String => timestamp.toLong / (60 * 60 * 24 * 7 * 30)
        case _ => throw new Exception("Type Unsupported")
    }
}

object TimeStampParser extends DataPreprocessor {
    override def preprocess(feature: Any): DateTime = feature match {
        case timestamp: String => new DateTime(timestamp.toLong * 1000L)
        case _ => throw new Exception("Type Unsupported")
    }
}

object DateTimeParser extends DataPreprocessor {
    override def preprocess(feature: Any): DateTime = feature match {
        case dateString: String => DateTime.parse(dateString, DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"))
        case _ => throw new Exception("Type Unsupported")
    }
}

object DateTimeIsWeekday extends DataPreprocessor {
    override def preprocess(feature: Any): Double = feature match {
        case datetime: DateTime => datetime.getDayOfWeek() match {
            case 1 | 2 | 3 | 4 | 5 => 1.0d
            case 6 | 7 => -1.0d
            case _ => 0d
        }
        case _ => throw new Exception("Type Unsupported")
    }
}

object DateTimeIsWeekend extends DataPreprocessor {
    override def preprocess(feature: Any): Double = DateTimeIsWeekday.preprocess(feature) * -1d
}

object DateTimeIsAfterOfficeHour extends DataPreprocessor {
    override def preprocess(feature: Any): Double = feature match {
        case datetime: DateTime => datetime.getHourOfDay() match {
            case x if (x >= 18) => 1d
            case y if (y < 18) => -1d
            case _ => 0d
        }
        case _ => throw new Exception("Type Unsupported")
    }
}

object distance {
    def compute (p1: (Double, Double), p2: (Double, Double)): Double = {
        val (p1x, p1y) = p1
        val (p2x, p2y) = p2
        val dx = p1x - p2x
        val dy = p1y - p2y
        Math.sqrt(dx*dx + dy*dy)
    }
}

class DistanceFromLivingArea (locationsByVenueId: Map[Int, (Double, Double)], locationsByUserId: Map[Int, (Double, Double)]) extends DataPreprocessor {
    override def preprocess(feature: Any): Double = feature match {
        case pair: String =>
            val userId = pair.split(',')(0).toInt
            val venueId = pair.split(',')(1).toInt

            distance.compute(
                locationsByVenueId(venueId),
                locationsByUserId(userId)
            )
        case _ => throw new Exception("Type Unsupported")
    }
}

class DistanceFromWorkingArea (locationsByVenueId: Map[Int, (Double, Double)], dbscanModel: DBSCANModel) extends DataPreprocessor {
    override def preprocess(feature: Any): Double = feature match {
        case pair: String =>
            val userId = pair.split(',')(0).toInt
            val venueId = pair.split(',')(1).toInt
            val venue = locationsByVenueId(venueId)

            dbscanModel.getClutsersByUserId(userId).values.map { cluster =>
                cluster.map {
                    p => distance.compute(locationsByVenueId(venueId), p)
                }.sum / cluster.size
            }.min
        case _ => throw new Exception("Type Unsupported")
    }
}