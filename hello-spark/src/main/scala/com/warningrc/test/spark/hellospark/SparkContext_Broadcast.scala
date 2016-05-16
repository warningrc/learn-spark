package com.warningrc.test.spark.hellospark

import MySpark._
/**
 * Broadcast Variables(广播变量)创建和使用<p>
 * 广播变量在广播后不能修改
 */
object SparkContext_Broadcast {
    def main(args: Array[String]): Unit = {
        val sc = getLocalSparkContext;
        val thisVal = Array(1, 2, 3, 4)
        val broadcastVal = sc.broadcast(thisVal)
        println(broadcastVal)
        assert(broadcastVal.value == thisVal)

        val intBroadCastVal = sc.broadcast(1);
        sc.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8, 9)).map { _ + intBroadCastVal.value }.foreach { println }
    }
}