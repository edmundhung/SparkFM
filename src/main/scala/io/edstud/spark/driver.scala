package io.edstud.spark

import org.apache.spark.{SparkConf, SparkContext, Logging}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import io.edstud.spark.fm._
import io.edstud.spark.fm.lib._
import io.edstud.spark.fm.util._
import breeze.linalg.SparseVector

object Application extends Logging  {

    def main (args: Array[String]) {

        val masterUrl = "local[*]"
        logInfo("Using MasterUrl - %s".format(masterUrl))

        val conf = Inertia.initializeSparkConf(masterUrl)
        val sc = new SparkContext(conf)

        //val rawData = getMovieLensDataset(sc)
        //FMUtils.saveAsLibFMFile(rawData, "%s/%s/libfm".format(root, MovieLens))

        val rmse = testALS(sc)

        sc.stop

    }

    private def testALS(sc: SparkContext): Double = {
        //val rawData = FMUtils.loadLibFMFile(sc, "%s/%s/libfm/part-00[0-9]*".format(root, MovieLens))
        val rawData = getMovieLensDataset(sc)
        //val rawData = getFoursquareDataset(sc)

        val collection = DataCollection.splitByRandom(
            rawData = rawData,
            trainWeight = 0.8,
            testWeight = 0.2
        )

        val model = FM(
            dataset = collection.trainingSet,
            numFactor = 2,
            maxIteration = 3
        ).learnWith(ALS.run)

        model.computeRMSE(collection.trainingSet)
        model.computeRMSE(collection.testSet)
    }

    //FMUtils.saveAsLibFMFile(rawData, "%s/%s/libfm".format(root, MovieLens))

    private def getMovieLensDataset(sc: SparkContext): RDD[(Double, SparseVector[Double])] = {
        logInfo("Reading MovieLens Dataset...")

        val usersFile = sc.textFile("%s/%s/users.dat".format(root, MovieLens)).map(_.split("::"))
        val itemsFile = sc.textFile("%s/%s/movies.dat".format(root, MovieLens)).map(_.split("::"))
        val ratingfile = sc.textFile("%s/%s/ratings.dat".format(root, MovieLens)).map(_.split("::"))

        new RelationVectorizor()

            .addRelation(usersFile, Map(
                0 -> DataNode.Target,
                1 -> DataNode.Identity,
                2 -> DataNode.Number,
                3 -> DataNode.Identity,
                4 -> DataNode.Identity
            ), 0)
            .addRelation(itemsFile, Map(
                0 -> DataNode.Target,
                1 -> DataNode.Bypass,
                2 -> DataNode.List.withSeperator("|")
            ), 1)

            .transform(ratingfile, Map(
                0 -> DataNode.Identity,
                1 -> DataNode.Identity,
                2 -> DataNode.Target,
                3 -> DataNode.Bypass//Time
            ))
    }

    private def getFoursquareDataset(sc: SparkContext): RDD[(Double, SparseVector[Double])] = {
        logInfo("Reading Foursquare Dataset...")

        val itemsFile = sc.textFile("%s/%s/venues.dat".format(root, Foursquare)).map(_.split('|')).persist(StorageLevel.MEMORY_AND_DISK_SER)
        val ratingfile = sc.textFile("%s/%s/ratings.dat".format(root, Foursquare)).map(_.split('|')).randomSplit(Array(0.1,0.9))

        val data = new RelationVectorizor()
            .addRelation(itemsFile, Map(
                0 -> DataNode.Target,
                1 -> DataNode.Number,
                2 -> DataNode.Number
            ), 1)
            .transform(ratingfile(0), Map(
                0 -> DataNode.Identity,
                1 -> DataNode.Identity,
                2 -> DataNode.Target
            ))

        data
    }

    private val root = "../../Dataset"
    private val MovieLens = "/MovieLens/ml-1m" //"ml-10M100K"
    private val Gowalla = "/Gowalla"
    private val Foursquare = "/Foursquare"

}

object Inertia {


    def initializeSparkConf(master: String): SparkConf = {
        val conf: SparkConf = new SparkConf().setAppName("Inertia").setMaster(master)
        FMUtils.registerKryoClasses(conf
            .set("spark.scheduler.mode", "FAIR")
            .set("spark.driver.memory", "2g")
            .set("spark.executor.memory", "2g")
            .set("spark.rdd.compress", "true")
            .set("spark.logConf", "true")
        )

        conf
    }

}
