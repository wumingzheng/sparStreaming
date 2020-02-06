package com.atguigu.spark.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object RDDQuereDemo {
  def main(args: Array[String]): Unit = {
    // TODO 创建上下问环境对象

    var conf: SparkConf = new SparkConf().setAppName("RDDQuere").setMaster("local[*]")

    var scc: StreamingContext = new StreamingContext(conf, Seconds(3))

    var sc: SparkContext = scc.sparkContext

    //创建一个可变队列
    var queue: mutable.Queue[RDD[Int]] = mutable.Queue[RDD[Int]]()

    var rddDS: InputDStream[Int] = scc.queueStream(queue, true)

    rddDS.reduce(_+_).print()

    scc.start()

    //循环的方式向队列中添加RDD
    for (elem <- 1 to 5){
      queue += sc.parallelize(1 to 100)
      Thread.sleep(2000)
    }

    scc.awaitTermination()
  }
}
