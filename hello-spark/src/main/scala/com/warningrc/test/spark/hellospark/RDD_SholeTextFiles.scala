package com.warningrc.test.spark.hellospark

import MySpark._

object RDD_SholeTextFiles {
    def main(args: Array[String]): Unit = {
        val sc = getLocalSparkContext()
        // 打印目录下所有文件的内容
        sc.wholeTextFiles(RDD_SholeTextFiles.getClass.getResource("/").getFile)
            .collect()
            .foreach({ value => println(value._1); println("-" * 20); println(value._2); println("=" * 20); })

        //打印目录下文件的大小
        sc.wholeTextFiles(RDD_SholeTextFiles.getClass.getResource("/").getFile)
            .map(v => (v._1, v._2.length()))
            .collect()
            .foreach(v => println(v._1 + ":" + v._2))
    }
}