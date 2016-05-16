package com.warningrc.test.spark.hellospark

import java.text.SimpleDateFormat
import java.util.Date
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.scheduler.InputFormatInfo
import org.apache.spark.sql.SQLContext
import MySpark._
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat
object SimpleApp {
    def main(args: Array[String]) {
        //        val logFile = "hdfs://w.hadoop1:9000/user/hadoop/testdata/core-site.xml"
        //        val sc = getSparkContext("spark://w.hadoop1:7077", "warning-test-SimpleApp")
        //        val textFile = sc.textFile(logFile).cache();
        //        textFile.flatMap(_.split(" ")).collect().foreach { println }
        //
        //        println("*" * 20)
        //
        //        textFile.flatMap { _.split(" ") }.map { (_, 1) }.reduceByKey(_ + _).sortBy(v => v._2, false).map(v => v._1).collect().foreach(println)

        val file = "hdfs://w.hadoop1:9000/user/hadoop/testdata/rss.log"
        val sc = new SparkContext(new SparkConf().setAppName("local-test")
            .setMaster("local[2]"))
        val conf = new Configuration()
        conf.setInt(NLineInputFormat.LINES_PER_MAP, 2);
        sc.newAPIHadoopFile(file, classOf[NLineInputFormat], classOf[LongWritable], classOf[Text], conf).map { value => value._2.toString }.collect().foreach({ text => println(text); println("xx" * 20) })
        
    }
}