package com.atguigu.spark.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.util.AccumulatorV2

object spark06_Acc {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("window").setMaster("local[*]")

    val sc: SparkContext = new SparkContext(conf)

    val dataRDD: RDD[Int] = sc.makeRDD(List(1,2))

    val acc = new MyAcc

    sc.register(acc)

    dataRDD.foreach(
      sum => {
        acc.add(sum)
      }
    )

    println(acc.value)



  }

  class MyAcc extends AccumulatorV2[Int,Long]{

    private var sum = 0L

    override def isZero: Boolean = sum == 0L

    override def copy(): AccumulatorV2[Int, Long] = {
      println("copy...")
      new MyAcc
    }

    override def reset(): Unit = {
      sum = 0L
    }

    override def add(v: Int): Unit = {
      sum = sum + v
    }

    override def merge(other: AccumulatorV2[Int, Long]): Unit = {
      sum = sum + other.value
    }

    override def value: Long = sum
  }
}
