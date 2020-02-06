package com.atguigu.spark.day02

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object spark01_transform {
  def main(args: Array[String]): Unit = {
    //创建环境对象
    var conf: SparkConf = new SparkConf().setAppName("transform").setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(3))

    val dstream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",9999)


    //Coding 这里可以写代码，在Driver中执行，执行一次
    val resultDstream = dstream.transform(rdd => {
      //Coding 这里可以写代码，在Driver中执行（只有在算子内部的代码执行），这个地方的代码会周期行的执行，这个地方的执行是采集周期的次数
      rdd
        .flatMap(_.split(" "))
        .map(
          //Coding 这里可以写代码，Executor中执行，有多少个任务就会执行多少次
          (_, 1)
        )
        .reduceByKey(_ + _)
    })

    resultDstream.print

//    dstream.foreachRDD(rdd => {
//      rdd.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).foreach(println)
//    })


    ssc.start()
    ssc.awaitTermination()
  }
}
