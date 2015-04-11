package io.edstud.spark

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

object config {
    val root: String = "../../Dataset"
    val seed: Long = 12345678
    val masterUrl = "yarn-client" // "local[*]"
    val MovieLens = "/MovieLens/10m" // "/MovieLens/ml-1m"
    val Foursquare = "/FourSquare" //"/Foursquare"
    val numFactor: Int = 10
    val maxIteration: Int = 100
}

object Inertia {

    def initializeSparkConf(master: String): SparkConf = {
        val conf: SparkConf = new SparkConf().setAppName("Inertia").setMaster(master)
        FMUtils.registerKryoClasses(conf
            .set("spark.scheduler.mode", "FAIR")
            .set("spark.driver.memory", "20g")
            .set("spark.executor.memory", "2g")
            .set("spark.rdd.compress", "true")
            .set("spark.logConf", "true")
        )

        conf
    }

}

object Application extends Logging  {

    def main (args: Array[String]) {

        val masterUrl = config.masterUrl
        logInfo("Using MasterUrl - %s".format(masterUrl))

        val conf = Inertia.initializeSparkConf(masterUrl)
        val sc = new SparkContext(conf)

        //FMUtils.saveAsLibFMFile(rawData, "%s/%s/libfm".format(root, MovieLens))
        //FMUtils.loadLibFMFile(sc, "%s/%s/libfm/part-00[0-9]*".format(root, MovieLens))



        //Foursquare.init(sc, 0.1, 5)

        //val (user, item) = Foursquare.baselineData(sc)
        val (user, item) = MovieLens.baselineData(sc)

        runBaseline(user, item, config.seed)

        val dataset = MovieLens.dataset0(sc)
        //val dataset = Foursquare.dataset0(sc)
        //val dataset = MovieLens.dataset1(sc)
        //val dataset = Foursquare.dataset1(sc)
        //val dataset = MovieLens.dataset2(sc)
        //val dataset = Foursquare.dataset2(sc)

        val collection = DataCollection.splitByRandom(dataset, config.seed, 0.6, 0.4)

        runALS(collection, config.numFactor, config.maxIteration)


        sc.stop

    }

    private def runBaseline(user: RDD[(Double, SparseVector[Double])], item: RDD[(Double, SparseVector[Double])], seed: Long) {
        Baseline.GlobalMean(DataCollection.splitByRandom(user, seed, 0.6, 0.4))
        Baseline.EntityMean(DataCollection.splitByRandom(user, seed, 0.6, 0.4), user)
        Baseline.EntityMean(DataCollection.splitByRandom(item, seed, 0.6, 0.4), item)
    }

    private def runALS(collection: DataCollection, numFactor: Int, maxIteration: Int): Double = {

        val model = FM(
            dataset = collection.trainingSet,
            numFactor = numFactor,
            maxIteration = maxIteration
        ).learnWith(ALS.run)

        model.computeRMSE(collection.trainingSet)
        model.computeRMSE(collection.testSet)
    }

}


object DistanceFromLivingArea extends DataTransformer {
    def transform(feature: String): Double = {
        val userId = feature.split(',')(0).toInt
        val venueId = feature.split(',')(1).toInt

        distance.compute(
            Foursquare.locationsByVenueId(venueId),
            Foursquare.locationsByUserId(userId)
        )
    }
}

object DistanceFromWorkingArea extends DataTransformer {
    def transform(feature: String): Double = {
        val userId = feature.split(',')(0).toInt
        val venueId = feature.split(',')(1).toInt
        val venue = Foursquare.locationsByVenueId(venueId)

        Foursquare.dbscanModel.head.getClutsersByUserId(userId).values.map { cluster =>
            cluster.map(p => distance.compute(venue, p)).sum / cluster.size
        }.min
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

object Foursquare extends Logging {

    val directory = config.Foursquare

    var checkinsByUserVenueIds: Map[(Int, Int), String] = Map[(Int, Int), String]()
    var locationsByVenueId: Map[Int, (Double, Double)] = Map[Int, (Double, Double)]()
    var locationsByUserId: Map[Int, (Double, Double)] = Map[Int, (Double, Double)]()

    var dbscanModel: Option[DBSCANModel] = None

    def init(sc: SparkContext, eps: Double, minPts: Int) {

        this.locationsByVenueId = this.grouping(sc, "venues.dat")
        this.locationsByUserId = this.grouping(sc, "users.dat")

        val checkins = this.dataFile(sc, "checkins.dat", 6).cache

        this.checkinsByUserVenueIds = checkins.map { items =>
            ((items(1).toInt, items(2).toInt), items(5))
        }.collectAsMap

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


        this.dbscanModel = Some(DBSCAN.byWorkingArea(checkinsByUserId, locationsByVenueId).learnWith(eps, minPts))
    }

    private def grouping(sc: SparkContext, file: String): Map[Int, (Double, Double)] = {
        this.dataFile(sc, file, 3).map {
            items => (items(0).toInt, (items(1).toDouble, items(2).toDouble))
        }.collectAsMap()
    }

    def dataFile(sc: SparkContext, file: String, size: Int): RDD[Array[String]] = {
        sc
        .textFile("%s/%s/%s".format(config.root, directory, file))
        .map(_.split('|').map(_.trim).filter(_.size > 0))
        .filter(_.size == size)
    }

    def dataset0(sc: SparkContext): RDD[(Double, SparseVector[Double])] = {
        logInfo("Reading Foursquare Dataset...")

        val ratingfile = this.dataFile(sc, "ratings.dat", 3).filter { items =>
            val UserVenueIds = (items(0).toInt, items(1).toInt)
            this.checkinsByUserVenueIds.contains(UserVenueIds)
        }

        val vectorizer = new StandardVectorizor()
        val data = vectorizer.transform(ratingfile, Map(
            0 -> DataNode.Identity,
            1 -> DataNode.Identity,
            2 -> DataNode.Target
        ))

        data
    }

    def dataset1(sc: SparkContext): RDD[(Double, SparseVector[Double])] = {
        logInfo("Reading Foursquare Dataset...")

        val userfile = this.dataFile(sc, "users.dat", 3)
        val itemFile = this.dataFile(sc, "venues.dat", 3)
        val ratingfile = this.dataFile(sc, "ratings.dat", 3).filter { items =>
            val UserVenueIds = (items(0).toInt, items(1).toInt)
            this.checkinsByUserVenueIds.contains(UserVenueIds)
        }

        val vectorizer = new RelationVectorizor()
        val data = vectorizer
            .addRelation(userfile, Map(
                0 -> DataNode.Target,
                1 -> DataNode.Number,
                2 -> DataNode.Number
            ), 0)
            .addRelation(itemFile, Map(
                0 -> DataNode.Target,
                1 -> DataNode.Number,
                2 -> DataNode.Number
            ), 1)
            .transform(ratingfile, Map(
                0 -> DataNode.Identity,
                1 -> DataNode.Identity,
                2 -> DataNode.Target
            ))

        data
    }

    def dataset2(sc: SparkContext): RDD[(Double, SparseVector[Double])] = {
        logInfo("Reading Foursquare Dataset...")

        val ratingfile = this.dataFile(sc, "ratings.dat", 3).filter { items =>
            val UserVenueIds = (items(0).toInt, items(1).toInt)
            this.checkinsByUserVenueIds.contains(UserVenueIds)
        }.map { items =>
            val UserVenueIds = (items(0).toInt, items(1).toInt)
            items :+ "%s,%s".format(items(0), items(1)) :+ this.checkinsByUserVenueIds(UserVenueIds)
        }

        val vectorizer = new StandardVectorizor()
        val data = vectorizer.transform(ratingfile, Map(
                0 -> DataNode.Identity,
                1 -> DataNode.Identity,
                2 -> DataNode.Target,
                3 -> DataNode.Number.withTransformer(DistanceFromLivingArea).withTransformer(DistanceFromWorkingArea),
                4 -> DataNode.Number.withTransformer(DateTimeIsAfterOfficeHour).withTransformer(DateTimeIsWeekend)
            ))

        data
    }

    def baselineData(sc: SparkContext): (RDD[(Double, SparseVector[Double])], RDD[(Double, SparseVector[Double])]) = {
        logInfo("Reading Foursquare Dataset...")
        val ratingfile = sc.textFile("%s/%s/ratings.dat".format(config.root, directory)).map(_.split('|')).cache

        val user = new StandardVectorizor().transform(ratingfile, Map(
            0 -> DataNode.Identity,
            1 -> DataNode.Bypass,
            2 -> DataNode.Target
        ))
        val item = new StandardVectorizor().transform(ratingfile, Map(
            0 -> DataNode.Bypass,
            1 -> DataNode.Identity,
            2 -> DataNode.Target
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

    val directory = config.MovieLens

    def dataset0(sc: SparkContext): RDD[(Double, SparseVector[Double])] = {
        logInfo("Reading MovieLens Dataset...")

        //val usersFile = sc.textFile("%s/%s/users.dat".format(config.root, directory)).map(_.split("::"))
        val itemsFile = sc.textFile("%s/%s/movies.dat".format(config.root, directory)).map(_.split("::"))
        val ratingfile = sc.textFile("%s/%s/ratings.dat".format(config.root, directory)).map(_.split("::"))

        val vectorizer = new RelationVectorizor()
        val data = vectorizer
            .transform(ratingfile, Map(
                0 -> DataNode.Identity,
                1 -> DataNode.Identity,
                2 -> DataNode.Target,
                3 -> DataNode.Bypass
            ))

        data
    }

    def dataset1(sc: SparkContext): RDD[(Double, SparseVector[Double])] = {
        logInfo("Reading MovieLens Dataset...")

        val itemsFile = sc.textFile("%s/%s/movies.dat".format(config.root, directory)).map(_.split("::"))
        val ratingfile = sc.textFile("%s/%s/ratings.dat".format(config.root, directory)).map(_.split("::"))

        val vectorizer = new RelationVectorizor()
        val data = vectorizer
            .addRelation(itemsFile, Map(
                0 -> DataNode.Target,
                1 -> DataNode.Bypass,
                2 -> DataNode.List.withSeperator("|")
            ), 1)
            .transform(ratingfile, Map(
                0 -> DataNode.Identity,
                1 -> DataNode.Identity,
                2 -> DataNode.Target,
                3 -> DataNode.Number
            ))

        data
    }

    def dataset2(sc: SparkContext): RDD[(Double, SparseVector[Double])] = {
        logInfo("Reading MovieLens Dataset...")

        val itemsFile = sc.textFile("%s/%s/movies.dat".format(config.root, directory)).map(_.split("::"))
        val ratingfile = sc.textFile("%s/%s/ratings.dat".format(config.root, directory)).map(_.split("::"))

        val vectorizer = new RelationVectorizor()
        val data = vectorizer
            .addRelation(itemsFile, Map(
                0 -> DataNode.Target,
                1 -> DataNode.Bypass,
                2 -> DataNode.List.withSeperator("|")
            ), 1)
            .transform(ratingfile, Map(
                0 -> DataNode.Identity,
                1 -> DataNode.Identity,
                2 -> DataNode.Target,
                3 -> DataNode.Number
                        .withTransformer(TimeStampIsAfterOfficeHour)
                        .withTransformer(TimeStampIsWeekend)
            ))

        data
    }

    def baselineData(sc: SparkContext): (RDD[(Double, SparseVector[Double])], RDD[(Double, SparseVector[Double])]) = {
        logInfo("Reading MovieLens Dataset...")
        val ratingfile = sc.textFile("%s/%s/ratings.dat".format(config.root, directory)).map(_.split("::")).cache
        val user = new StandardVectorizor().transform(ratingfile, Map(
            0 -> DataNode.Identity,
            1 -> DataNode.Bypass,
            2 -> DataNode.Target,
            3 -> DataNode.Bypass
        ))
        val item = new StandardVectorizor().transform(ratingfile, Map(
            0 -> DataNode.Bypass,
            1 -> DataNode.Identity,
            2 -> DataNode.Target,
            3 -> DataNode.Bypass
        ))


        (user, item)
    }


}
