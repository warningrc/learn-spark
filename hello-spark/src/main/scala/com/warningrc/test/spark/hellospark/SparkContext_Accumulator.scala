package com.warningrc.test.spark.hellospark

import scala.annotation.elidable
import scala.annotation.elidable.ASSERTION
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.AccumulatorParam

import MySpark.getLocalSparkContext
/**
 * Accumulators(累加器)
 */
object SparkContext_Accumulator {
    def main(args: Array[String]): Unit = {
        val sc = getLocalSparkContext;
        // 基本数据类型的累加器
        val accumulatorVal = sc.accumulator(0, "my accumulator")
        val seq = ArrayBuffer[Int]()
        var loacalVal = 0;
        sc.parallelize(0 to 10, 4).foreach { item => accumulatorVal.add(item); loacalVal = loacalVal + item }

        println(loacalVal)
        println(accumulatorVal.value)
        assert(loacalVal == 0)
        assert(accumulatorVal.value == (0 to 10).sum)

        // 基础集合类的累加器
        val accumulatorSeq = sc.accumulableCollection(seq);
        sc.parallelize(0 to 10, 4).foreach { accumulatorSeq.+=(_) }
        println(accumulatorSeq.value)
        assert(accumulatorSeq.value == (0 to 10))

        // 自定义累加器
        val accumulatorValue = sc.accumulable(new AccumulatorValue(0))(VectorAccumulatorParam)
        sc.parallelize(AccumulatorValue(Array(1, 2, 3, 4, 5, 6, 7, 8, 9)), 4).foreach { accumulatorValue.add(_) }
        println(accumulatorValue.value.number)
        assert(accumulatorValue.value.number == (1 to 9).sum)

    }
    class AccumulatorValue(var number: Int) extends Serializable {
        def +(other: AccumulatorValue): AccumulatorValue = {
            new AccumulatorValue(this.number + other.number)
        }
    }
    object AccumulatorValue {
        def apply(numbers: Array[Int]): Array[AccumulatorValue] = {
            for (number <- numbers) yield new AccumulatorValue(number)
        }
    }
    object VectorAccumulatorParam extends AccumulatorParam[AccumulatorValue] {
        def zero(initialValue: AccumulatorValue): AccumulatorValue = {
            initialValue
        }
        def addInPlace(v1: AccumulatorValue, v2: AccumulatorValue): AccumulatorValue = {
            v1 + v2
        }
    }
}
