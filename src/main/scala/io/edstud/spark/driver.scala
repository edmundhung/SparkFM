package io.edstud.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import io.edstud.spark.fm._
import io.edstud.spark.fm.lib._

object Application {

    def main (args: Array[String]) {
        val conf = Inertia.initializeSparkConf("local")
        val sc = new SparkContext(conf)
        val rawData = FMUtils.loadLibFMFile(sc, path)
        val collection = DataCollection(rawData, 0.8, 0.2).cache()
        val model = FM(collection.trainingSet, Task.Regression).learnWith(ALS.run(8, 100))
        val (rmse, mae) = model.evaluateRegression(collection.testSet)

    }

    private def path = "/home/edmund/Workspace/FYP/Dataset/Gowalla/loc-gowalla_totalCheckins.txt"

}

object Inertia {

    def initializeSparkConf(master: String): SparkConf = {
        val conf: SparkConf = new SparkConf().setAppName("Inertia").setMaster(master).set("spark.scheduler.mode", "FAIR")
        FMUtils.registerKryoClasses(conf)

        conf
    }

}