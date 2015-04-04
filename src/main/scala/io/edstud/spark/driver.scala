package io.edstud.spark

import org.apache.spark.{SparkConf, SparkContext, Logging}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import io.edstud.spark.fm._
import io.edstud.spark.fm.lib._
import io.edstud.spark.fm.util._

object Application extends Logging  {

    def main (args: Array[String]) {

        val masterUrl = "local[*]"
        logInfo("Using MasterUrl - %s".format(masterUrl))

        val conf = Inertia.initializeSparkConf(masterUrl)
        val sc = new SparkContext(conf)

        //StandardVectorizor(sc)
        //RelationVectorizor(sc)

        val rmse = testALS(sc)

        sc.stop

    }

    private def RelationVectorizor(sc: SparkContext) {

        val userFile = sc.textFile("%s/users.dat".format(directory)).map(_.split("::"))
        val movieFile = sc.textFile("%s/movies.dat".format(directory)).map(_.split("::"))
        val ratingfile = sc.textFile("%s/ratings.dat".format(directory)).map(_.split("::"))

        val rawData = new RelationVectorizor()
                            .addRelation(userFile, Map(
                                0 -> DataNode.Target,
                                1 -> DataNode.Identity,
                                2 -> DataNode.Number,
                                3 -> DataNode.Identity,
                                4 -> DataNode.Identity
                            ), 0)
                            .addRelation(movieFile, Map(
                                0 -> DataNode.Target,
                                1 -> DataNode.Identity,
                                2 -> DataNode.List.withSeperator("|")
                            ), 1)
                            .transform(ratingfile, Map(
                                0 -> DataNode.Identity,
                                1 -> DataNode.Identity,
                                2 -> DataNode.Target,
                                3 -> DataNode.Time
                            ))

        FMUtils.saveAsLibFMFile(rawData, "%s/libfm".format(directory))
    }

    private def StandardVectorizor(sc: SparkContext) {
        val rawfile = sc.textFile("%s/ratings.dat".format(directory)).map(_.split("::"))
        val definitions: Map[Int, DataNode] = Map(
            0 -> DataNode.Identity,
            1 -> DataNode.Identity,
            2 -> DataNode.Target,
            3 -> DataNode.Time
        )

        val vectorizor = new StandardVectorizor()
        val rawData = vectorizor.transform(rawfile, definitions).cache

        FMUtils.saveAsLibFMFile(rawData, "%s/libfm".format(directory))
    }

    private def testALS(sc: SparkContext): Double = {
        val userFile = sc.textFile("%s/users.dat".format(directory)).map(_.split("::"))
        val movieFile = sc.textFile("%s/movies.dat".format(directory)).map(_.split("::"))
        val ratingfile = sc.textFile("%s/ratings.dat".format(directory)).map(_.split("::"))

        val rawData = new RelationVectorizor()
                            .addRelation(userFile, Map(
                                0 -> DataNode.Target,
                                1 -> DataNode.Identity,
                                2 -> DataNode.Number,
                                3 -> DataNode.Identity,
                                4 -> DataNode.Identity
                            ), 0)
                            /*
                            .addRelation(movieFile, Map(
                                0 -> DataNode.Target,
                                1 -> DataNode.Identity,
                                2 -> DataNode.List.withSeperator("|")
                            ), 1)
                            */
                            .transform(ratingfile, Map(
                                0 -> DataNode.Identity,
                                1 -> DataNode.Identity,
                                2 -> DataNode.Target,
                                3 -> DataNode.Time
                            ))

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

        model.computeRMSE(collection.testSet)
    }

    private val directory = "/home/edmund/Workspace/FYP/Dataset/MovieLens/ml-1m"

}

object Inertia {


    def initializeSparkConf(master: String): SparkConf = {
        val conf: SparkConf = new SparkConf().setAppName("Inertia").setMaster(master)
        FMUtils.registerKryoClasses(conf.set("spark.scheduler.mode", "FAIR").set("spark.logConf", "true"))

        conf
    }

}
