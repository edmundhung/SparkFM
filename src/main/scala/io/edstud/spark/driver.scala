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

        //testVectorizor(sc)
        val rmse = testALS(sc)

        sc.stop

    }

    private def testVectorizor(sc: SparkContext) {
        val rawfile = sc.textFile("%s/ratings.dat".format(directory)).map(_.split("::"))
        val definitions: Map[Int, DataNode] = Map(
            0 -> DataNode.Identity,
            1 -> DataNode.Identity,
            2 -> DataNode.Target,
            3 -> DataNode.Number
        )

        val vectorizor = new StandardVectorizor()
        val resultRDD = vectorizor.transform(rawfile, definitions)

        logInfo("Vectorisation Completed")

        FMUtils.saveAsLibFMFile(resultRDD, "%s/libfm".format(directory))
    }

    private def testALS(sc: SparkContext): Double = {
        //val rawData = FMUtils.loadLibFMFile(sc, "%s/ratings.dat.libfm".format(directory))

        val rawfile = sc.textFile("%s/ratings.dat".format(directory)).map(_.split("::"))
        val definitions: Map[Int, DataNode] = Map(
            0 -> DataNode.Identity,
            1 -> DataNode.Identity,
            2 -> DataNode.Target,
            3 -> DataNode.Number
        )

        val vectorizor = new StandardVectorizor()
        val rawData = vectorizor.transform(rawfile, definitions)

        val collection = DataCollection.splitByRandom(
            rawData = rawData,
            trainWeight = 0.8,
            testWeight = 0.2
        )

        val model = FM(
            dataset = collection.trainingSet,
            numFactor = 6,
            maxIteration = 3
        ).learnWith(ALS.run)

        model.computeRMSE(collection.testSet)
    }

    private def directory = "/home/edmund/Workspace/FYP/Dataset/MovieLens/ml-1m"


}

object Inertia {


    def initializeSparkConf(master: String): SparkConf = {
        val conf: SparkConf = new SparkConf().setAppName("Inertia").setMaster(master)
        FMUtils.registerKryoClasses(conf.set("spark.scheduler.mode", "FAIR").set("spark.logConf", "true"))

        conf
    }

}
