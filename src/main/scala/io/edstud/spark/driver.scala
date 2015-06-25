package io.edstud.spark

import scala.util.Try
import scala.collection.Map
import org.apache.spark.{SparkConf, SparkContext, Logging}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import com.github.nscala_time.time.Imports._
import io.edstud.spark.fm._
import io.edstud.spark.fm.lib._
import io.edstud.spark.fm.util._
import io.edstud.spark.dbscan._
import io.edstud.spark.baseline._
import breeze.linalg.SparseVector

object FMConfig {
    val root: String = "../../Dataset" // "s3n://project-inertia/Dataset"
    val seed: Long = 52577450
    val masterUrl = "local[*]" // "yarn-client"
    val MovieLens = "/MovieLens/1m" // "/MovieLens/ml-1m"
    val Foursquare = "/FourSquare" //"/Foursquare"
    val numFactor: Int = 10
    val maxIteration: Int = 100
}

object Inertia {

    def initializeSparkConf(master: String): SparkConf = {
        val conf: SparkConf = new SparkConf()
            .setAppName("Inertia")
            .setMaster(master)
            .set("spark.driver.memory", "4g")
            .set("spark.executor.memory", "4g")
            .set("spark.storage.memoryFraction", "0.8")
            //.set("spark.rdd.compress", "true")
            //.set("spark.scheduler.mode", "FAIR")

        FMUtils.registerKryoClasses(conf)

        conf
    }

}

object Application extends Logging  {

    def main (args: Array[String]) {

        val masterUrl = FMConfig.masterUrl
        logInfo("Using MasterUrl - %s".format(masterUrl))

        val conf = Inertia.initializeSparkConf(masterUrl)
        val sc = new SparkContext(conf)

        //FMUtils.saveAsLibFMFile(rawData, "%s/%s/libfm".format(root, MovieLens))
        //FMUtils.loadLibFMFile(sc, "%s/%s/libfm/part-00[0-9]*".format(root, MovieLens))

        if (args.size == 3) {

            var data = args(0)
            var step = args(1).toInt
            var configs = args(2).split(':')

            val seed = configs(0).toLong
            val numFactor = configs(1).toInt
            val maxIteration = configs(2).toInt

            if (data.contains("FS")) {
                val setup = data.split("FS")(1).split(':')
                val (eps, minPts) = (setup(0).toDouble, setup(1).toInt)
                Foursquare.init(sc, eps, minPts, step)

                data = "FS"
            }

            if (step == -1) {

                val (user, item) = data match {
                    case "FS" => Foursquare.baselineData(sc)
                    case "ML" => MovieLens.baselineData(sc)
                }

                runBaseline(user, item, seed)

            } else {

                val dataset = step match {
                    case 0 => data match {
                        case "FS" => Foursquare.dataset0(sc)
                        case "ML" => MovieLens.dataset0(sc)
                    }
                    case 1 => data match {
                        case "FS" => Foursquare.dataset1(sc)
                        case "ML" => MovieLens.dataset1(sc)
                    }
                    case 2 => data match {
                        case "FS" => Foursquare.dataset2(sc)
                        case "ML" => MovieLens.dataset2(sc)
                    }
                }

                //FMUtils.saveAsLibFMFile(dataset, "%s/output/libfm".format(FMConfig.root))

                val vectorizer = data match {
                    case "FS" => Foursquare.vectorizer.get
                    case "ML" => MovieLens.vectorizer.get
                }

                val collection = DataCollection.splitByRandom(dataset, seed, 0.6, 0.4)

                logInfo("Training size = " + collection.trainingSet.size)
                logInfo("Test size = " + collection.testSet.size)

                val model = FM(
                    dataset = collection.trainingSet,
                    numFactor = numFactor,
                    maxIteration = maxIteration
                ).withVectorizer(vectorizer).learnWith(ALS.run)

                model.computeRMSE(collection.trainingSet)
                model.computeRMSE(collection.testSet)

                logInfo("Recommendations for UserId (%d)".format(1))
                model.recommendItems(sc, Map[String, Any](
                    "UserId" -> 1,
                    "Time1" -> 998302268,
                    "Time2" -> 998302268
                ), 10).zipWithIndex.map { case ((itemId, rating), index) =>
                    "Rank %d: Item %s".format(index + 1, itemId)
                }.foreach(msg => logInfo(msg))

            }


        }


        sc.stop

    }

    private def runBaseline(user: RDD[(Double, SparseVector[Double])], item: RDD[(Double, SparseVector[Double])], seed: Long) {
        Baseline.GlobalMean(DataCollection.splitByRandom(user, seed, 0.6, 0.4))
        Baseline.EntityMean(DataCollection.splitByRandom(user, seed, 0.6, 0.4), user)
        Baseline.EntityMean(DataCollection.splitByRandom(item, seed, 0.6, 0.4), item)
    }

}

object Foursquare extends Logging {

    val directory = FMConfig.Foursquare

    var checkinsByUserVenuePair: Map[(Int, Int), String] = Map[(Int, Int), String]()
    var locationsByVenueId: Map[Int, (Double, Double)] = Map[Int, (Double, Double)]()
    var locationsByUserId: Map[Int, (Double, Double)] = Map[Int, (Double, Double)]()

    var dbscanModel: Option[DBSCANModel] = None

    var vectorizer: Option[StandardVectorizer] = None

    def init(sc: SparkContext, eps: Double, minPts: Int, cat: Int = 2) {

        if (cat == 2) {
            logDebug("Materializing Venue Location Mapping")
            this.locationsByVenueId = this.grouping(sc, "Venues")

            logDebug("Materializing User Location Mapping")
            this.locationsByUserId = this.grouping(sc, "Users")
        }

        val checkins = this.dataFile(sc, "Checkins", 6).cache

        logDebug("Materializing Checkin Time Mapping")
        this.checkinsByUserVenuePair = checkins.map { items =>
            ((items(1).toInt, items(2).toInt), items(5))
        }.collectAsMap

        if (cat == 2) {
            val checkinsByUserId = checkins.map {
                items => (
                    items(1).toInt, (
                        items(2).toInt,
                        DateTime.parse(items(5), DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"))
                    )
                )
            }.filter { case (userId, (venueId, datetime)) =>
                locationsByUserId.contains(userId) && locationsByVenueId.contains(venueId)
            }.groupByKey.mapValues(_.toArray)

            logDebug("Identifying Significant locations with DBSCAN")
            this.dbscanModel = Some(DBSCAN.byWorkingArea(checkinsByUserId, locationsByVenueId).learnWith(eps, minPts))
        }

        logDebug("Initialization Completed")
    }

    private def grouping(sc: SparkContext, file: String): Map[Int, (Double, Double)] = {
        this.dataFile(sc, file, 3).map {
            items => (items(0).toInt, (items(1).toDouble, items(2).toDouble))
        }.collectAsMap()
    }

    def dataFile(sc: SparkContext, name: String, size: Int): RDD[Array[String]] = {
        sc
        .textFile("%s/%s/%s.dat".format(FMConfig.root, directory, name.toLowerCase))
        .map(_.split('|').map(_.trim).filter(_.size > 0))
        .filter(_.size == size)
        .setName(name)
    }

    def dataset0(sc: SparkContext): RDD[(Double, SparseVector[Double])] = {
        logInfo("Reading Foursquare Dataset...")

        val ratingfile = this.dataFile(sc, "Ratings", 3).filter { items =>
            val UserVenueIds = (items(0).toInt, items(1).toInt)
            this.checkinsByUserVenuePair.contains(UserVenueIds)
        }

        vectorizer = Some(new StandardVectorizer())
        val data = vectorizer.get.transform(ratingfile, Map(
            0 -> DataDomain.Categorical,
            1 -> DataDomain.Categorical,
            2 -> DataDomain.Rating
        ))

        data
    }

    def dataset1(sc: SparkContext): RDD[(Double, SparseVector[Double])] = {
        logInfo("Reading Foursquare Dataset...")

        //val userfile = this.dataFile(sc, "Users", 3)
        val itemFile = this.dataFile(sc, "Venues", 3)
        val ratingfile = this.dataFile(sc, "Ratings", 3).filter { items =>
            val UserVenueIds = (items(0).toInt, items(1).toInt)
            this.checkinsByUserVenuePair.contains(UserVenueIds)
        }

        vectorizer = Some(
            new RelationVectorizer()
        /*
            .addRelation(userfile, Map(
                0 -> DataDomain.Rating,
                1 -> DataDomain.RealValued,
                2 -> DataDomain.RealValued
            ), 0)
        */
            .addRelation(itemFile, Map(
                0 -> DataDomain.Rating,
                1 -> DataDomain.RealValued,
                2 -> DataDomain.RealValued
            ), 1)
        )
        val data = vectorizer.get.transform(ratingfile, Map(
            0 -> DataDomain.Categorical,
            1 -> DataDomain.Categorical,
            2 -> DataDomain.Rating
        ))

        data
    }

    def dataset2(sc: SparkContext): RDD[(Double, SparseVector[Double])] = {
        logInfo("Reading Foursquare Dataset...")

        val ratingfile = this.dataFile(sc, "Ratings", 3).filter { items =>
            val UserVenueIds = (items(0).toInt, items(1).toInt)
            this.checkinsByUserVenuePair.contains(UserVenueIds)
        }.map { items =>
            val userVenuePair = "%s,%s".format(items(0), items(1))
            val checkinTime = this.checkinsByUserVenuePair((items(0).toInt, items(1).toInt))
            items :+ userVenuePair :+ userVenuePair :+ checkinTime :+ checkinTime
        }

        vectorizer = Some(new StandardVectorizer())
        val data = vectorizer.get.transform(ratingfile, Map(
                0 -> DataDomain.Categorical,
                1 -> DataDomain.Categorical,
                2 -> DataDomain.Rating,
                3 -> DataDomain.RealValued
                    .withPreprocessor(
                        new DistanceFromLivingArea(Foursquare.locationsByVenueId, Foursquare.locationsByUserId)
                    ),
                4 -> DataDomain.RealValued
                    .withPreprocessor(
                        new DistanceFromWorkingArea(Foursquare.locationsByVenueId, Foursquare.dbscanModel.head)
                    ),
                5 -> DataDomain.RealValued
                    .withPreprocessor(DateTimeParser)
                    .withPreprocessor(DateTimeIsAfterOfficeHour),
                6 -> DataDomain.RealValued
                    .withPreprocessor(DateTimeParser)
                    .withPreprocessor(DateTimeIsWeekend)
            ))

        data
    }

    def baselineData(sc: SparkContext): (RDD[(Double, SparseVector[Double])], RDD[(Double, SparseVector[Double])]) = {
        logInfo("Reading Foursquare Dataset...")

        val ratingfile = this.dataFile(sc, "Ratings", 3).filter { items =>
            val UserVenueIds = (items(0).toInt, items(1).toInt)
            this.checkinsByUserVenuePair.contains(UserVenueIds)
        }.cache

        val user = new StandardVectorizer().transform(ratingfile, Map(
            0 -> DataDomain.Categorical,
            1 -> DataDomain.Bypass,
            2 -> DataDomain.Rating
        ))

        val item = new StandardVectorizer().transform(ratingfile, Map(
            0 -> DataDomain.Bypass,
            1 -> DataDomain.Categorical,
            2 -> DataDomain.Rating
        ))

        (user, item)
    }

}


object Baseline extends Logging {

    def GlobalMean(collection: DataCollection): Double = {
        new GlobalMeanModel(collection.trainingSet.targets.mean).computeRMSE(collection.testSet)
    }

    def EntityMean(collection: DataCollection, raw: RDD[(Double, SparseVector[Double])]): Double = {
        new EntityMeanModel(raw.filter(_._2.activeSize == 1).mapValues {
            features => features.activeKeysIterator.mkString.toInt
        }.map(_.swap).groupByKey().mapValues { list =>
            val sum = list.reduce(_+_)
            val size = list.size
            val mean = sum / size

            mean
        }.collectAsMap).computeRMSE(collection.testSet)
    }

}

object MovieLens extends Logging {

    val directory = FMConfig.MovieLens

    var vectorizer: Option[StandardVectorizer] = None

    def dataFile(sc: SparkContext, name: String, size: Int): RDD[Array[String]] = {
        sc
        .textFile("%s/%s/%s.dat".format(FMConfig.root, directory, name.toLowerCase))
        .map(_.split("::").map(_.trim).filter(_.size > 0))
        .filter(_.size == size)
        .setName(name)
    }

    def dataset0(sc: SparkContext): RDD[(Double, SparseVector[Double])] = {
        logInfo("Reading MovieLens Dataset...")

        val ratingfile = this.dataFile(sc, "Ratings", 4)

        vectorizer = Some(new StandardVectorizer())
        val data = vectorizer.get.transform(ratingfile, Map(
            0 -> DataDomain.Categorical.withName("UserId"),
            1 -> DataDomain.Categorical.withName("ItemId"),
            2 -> DataDomain.Rating,
            3 -> DataDomain.Bypass
        ))

        data
    }

    def dataset1(sc: SparkContext): RDD[(Double, SparseVector[Double])] = {
        logInfo("Reading MovieLens Dataset...")

        val itemsFile = this.dataFile(sc, "Movies", 3)
        val ratingfile = this.dataFile(sc, "Ratings", 4)

        vectorizer = Some(
            new RelationVectorizer()
            .addRelation(itemsFile, Map(
                0 -> DataDomain.Rating,
                1 -> DataDomain.Bypass,
                2 -> DataDomain.CategoricalSet("|")
            ), 1)
        )

        val data = vectorizer.get.transform(ratingfile, Map(
            0 -> DataDomain.Categorical.withName("UserId"),
            1 -> DataDomain.Categorical.withName("ItemId"),
            2 -> DataDomain.Rating,
            3 -> DataDomain.RealValued.withPreprocessor(TimeStampToMonth).withName("Time1")
        ))

        data
    }

    def dataset2(sc: SparkContext): RDD[(Double, SparseVector[Double])] = {
        logInfo("Reading MovieLens Dataset...")

        val usersFile = this.dataFile(sc, "Users", 5)
        val itemsFile = this.dataFile(sc, "Movies", 3)
        val ratingfile = this.dataFile(sc, "Ratings", 4).map( items => items :+ items(3))

        vectorizer = Some(
            new RelationVectorizer()
            .addRelation(usersFile, Map(
                0 -> DataDomain.Rating,
                1 -> DataDomain.Categorical,
                2 -> DataDomain.RealValued,
                3 -> DataDomain.Categorical,
                4 -> DataDomain.Categorical
            ), 0)
            .addRelation(itemsFile, Map(
                0 -> DataDomain.Rating,
                1 -> DataDomain.Bypass,
                2 -> DataDomain.CategoricalSet("|")
            ), 1)
        )

        val data = vectorizer.get.transform(ratingfile, Map(
            0 -> DataDomain.Categorical.withName("UserId"),
            1 -> DataDomain.Categorical.withName("ItemId"),
            2 -> DataDomain.Rating,
            3 -> DataDomain.RealValued.withName("Time1")
                .withPreprocessor(TimeStampParser).withPreprocessor(DateTimeIsAfterOfficeHour),
            4 -> DataDomain.RealValued.withName("Time2")
                .withPreprocessor(TimeStampParser).withPreprocessor(DateTimeIsWeekend)
        ))

        data
    }

    def baselineData(sc: SparkContext): (RDD[(Double, SparseVector[Double])], RDD[(Double, SparseVector[Double])]) = {
        logInfo("Reading MovieLens Dataset...")

        val ratingfile = this.dataFile(sc, "Ratings", 4).cache

        val user = new StandardVectorizer().transform(ratingfile, Map(
            0 -> DataDomain.Categorical,
            1 -> DataDomain.Bypass,
            2 -> DataDomain.Rating,
            3 -> DataDomain.Bypass
        ))

        val item = new StandardVectorizer().transform(ratingfile, Map(
            0 -> DataDomain.Bypass,
            1 -> DataDomain.Categorical,
            2 -> DataDomain.Rating,
            3 -> DataDomain.Bypass
        ))


        (user, item)
    }


}
