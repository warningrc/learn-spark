package com.warningrc.test.spark.hellospark

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object MySpark {
    def getSparkContext(master: String, appName: String): SparkContext = {
        val sc = new SparkContext(new SparkConf().setAppName(appName)
            .setMaster(master) //            .set("spark.executor.memory", "512m")
            )
        //sc.addJar("./target/hello-spark-1.0.jar")
        sc
    }

    def getLocalSparkContext(count: Int = 2, appName: String = "local-test", memory: String = "100m"): SparkContext = {
        new SparkContext(new SparkConf().setAppName(appName)
            .setMaster(s"local[$count]").set("spark.executor.memory", memory))
    }
}