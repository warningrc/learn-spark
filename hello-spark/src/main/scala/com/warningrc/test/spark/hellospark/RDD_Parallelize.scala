package com.warningrc.test.spark.hellospark
import MySpark._
object RDD_Parallelize {
    def main(args: Array[String]): Unit = {
        val sc = getLocalSparkContext()
        val data = Array(1, 2, 3, 4, 5, 6, 7, 8, 9)
        (sc parallelize data).filter { _ % 2 == 0 }.collect() foreach println
        println("*" * 20)
        sc.parallelize(data, 2).collect() foreach println
        sc.parallelize(data, 3).collect() foreach println
        println((sc parallelize data).reduce(_ + _))
    }
}