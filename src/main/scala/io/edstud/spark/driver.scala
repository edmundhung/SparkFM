package io.edstud.spark

import org.apache.spark.{SparkConf, SparkContext, Logging}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import io.edstud.spark.fm._
import io.edstud.spark.fm.lib._

object Application extends Logging  {

    def main (args: Array[String]) {
        val conf = Inertia.initializeSparkConf("local")
        val sc = new SparkContext(conf)
        val rawData = FMUtils.loadLibFMFile(sc, path)
        val collection = DataCollection.byRandomSplit(rawData, 0.9, 0.1)
        val model = FM(collection.trainingSet.cache, Task.Regression).learnWith(ALS.run(8, 100))
        val (rmse, mae) = model.evaluateRegression(collection.testSet.cache)

        sc.stop

    }

    private def path = "/home/edmund/Workspace/FYP/Dataset/MovieLens/ml-1m/ratings.dat.libfm"

}

object Inertia {

    def initializeSparkConf(master: String): SparkConf = {
        val conf: SparkConf = new SparkConf().setAppName("Inertia").setMaster(master)
        FMUtils.registerKryoClasses(conf.set("spark.scheduler.mode", "FAIR").set("spark.logConf", "true"))

        conf
    }

}