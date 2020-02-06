package com.atguigu.spark.day02

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object spark04_window {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("window").setMaster("local[*]")

    val ssc = new StreamingContext(conf,Seconds(3))

    val socketDS: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",9999)

    // TODO 窗口的大小以及滑动 的大小因该是采集周期的整数倍
    socketDS.window(Seconds(6),Seconds(3))

    val resultDS: DStream[(String, Int)] = socketDS.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    resultDS.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
