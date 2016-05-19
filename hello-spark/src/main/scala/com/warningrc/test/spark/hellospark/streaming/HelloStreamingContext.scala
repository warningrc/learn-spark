package com.warningrc.test.spark.hellospark.streaming
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import com.warningrc.test.spark.hellospark.MySpark._
import org.apache.spark.rdd.RDD
import java.io.Serializable

/**
 *
 * 从socket中获取数据并执行计算操作。
 *
 * 使用 nc -lk 9999 模拟socket
 *
 */
object HelloStreamingContext {
    def main(args: Array[String]): Unit = {
        //        val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
        //        val ssc = new StreamingContext(conf, Seconds(5))
        // 流处理执行间隔时间是5秒
        val ssc = new StreamingContext(getLocalSparkContext(appName = "NetworkWordCount"), Seconds(5))
        // 定义输入源(从socket获取文本数据)
        val lines = ssc.socketTextStream("w.hadoop1", 9999);
        // 定义流计算指令
        val words = lines.flatMap(_.split(" ")).filter(_ != " ").map { (_, 1) }.cache()

        words.reduceByKey(_ + _).print()

        words.reduceByKeyAndWindow((a: Int, b: Int) => a + b, Seconds(10), Seconds(5))
            .foreachRDD((rdd: RDD[(String, Int)]) => {
                // 这种方式需要将connection序列化然后分发到各个worker中,可能会发生异常,不推荐使用
                val connection = createNewConnection
                rdd.foreach(connection.send(_))
            })
        words.reduceByKeyAndWindow((a: Int, b: Int) => a + b, Seconds(10), Seconds(5))
            .foreachRDD((rdd: RDD[(String, Int)]) => {
                // 这种方式在每条数据中都构建一个connection,导致资源耗尽,影响整个系统的吞吐量,不推荐使用
                rdd.foreach(it => {
                    val connection = createNewConnection
                    connection.send(it)
                })
            })
        words.reduceByKeyAndWindow((a: Int, b: Int) => a + b, Seconds(10), Seconds(5))
            .foreachRDD((rdd: RDD[(String, Int)]) => {
                rdd.foreachPartition(partitionOfRecords => {
                    // 使用连接池,将对象改为延迟创建等行为可以优化整个系统性能
                    lazy val connection = createNewConnection
                    partitionOfRecords.foreach(connection.send(_))
                })
            })

        // 开始接收和处理数据
        ssc.start()
        // 处理过程将一直持续，直到streamingContext.stop()方法被调用
        ssc.awaitTermination()
    }

    def createNewConnection(): Connection = new Connection

    /**
     * 模拟网络连接
     */
    class Connection extends Serializable {
        /**
         * 模拟向网络发送数据
         */
        def send(value: Any): Unit = {
            // do something
            println(value)
        }
    }
}