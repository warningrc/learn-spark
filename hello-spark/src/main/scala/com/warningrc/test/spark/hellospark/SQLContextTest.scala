package com.warningrc.test.spark.hellospark

import java.text.SimpleDateFormat
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object SQLContextTest {
    def main(args: Array[String]): Unit = {
        val sc = new SparkContext(new SparkConf().setAppName("local-test-sql")
            .setMaster("local[2]"))
        sqlContext(sc)
    }

    def sqlContext(sc: SparkContext) = {
        val file = SimpleApp.getClass.getResource("/Rss.log").getFile
        val textFile = sc.textFile(file)
        val sqlContext = new SQLContext(sc)
        import sqlContext.implicits._
        val rssRdd = textFile.filter { line => line.indexOf("MainThread") > -1 }.map { parseRss(_) }.toDF()
        rssRdd.registerTempTable("rss")
        sqlContext.sql("select * from rss where thread.number = 1788").collect().foreach { it => println(it(1)) }
    }

    def parseRss(line: String): Rss = {
        val p = line.replaceAll("\\[", " ").replaceAll("\\]", " ").replaceAll(",", " ")
            .split(" ").filter { _.trim().length() > 0 }
        Rss(Array(p(0).toInt, p(1).toInt, p(2).toInt, p(3).toInt), ThreadValue(p(4).replace(":", ""), p(5).toInt), new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS").parse(p(6) + " " + p(7)).getTime)
    }
}

case class Rss(number: Array[Int], thread: ThreadValue, time: Long)
case class ThreadValue(name: String, number: Long)
