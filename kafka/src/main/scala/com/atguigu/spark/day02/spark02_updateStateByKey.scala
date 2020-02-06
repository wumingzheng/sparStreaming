package com.atguigu.spark.day02

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object spark02_updateStateByKey {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("updateStateByKey")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))
    ssc.sparkContext.setCheckpointDir("E:\\MyWork\\WorkSpace\\sparStreaming\\kafka\\input")

    val dstream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",9999)

    val wordDS: DStream[String] = dstream.flatMap(_.split(" "))

    val mapDS: DStream[(String, Int)] = wordDS.map(
      word => {
        (word, 1)
      }
    )


    /*
    *
    * seq : 如果第一个离散化流中有几次单词出现seq就是几个1
    * buffer : 第一次没值
    *
    *
    */
    val stateDS: DStream[(String, Long)] = mapDS.updateStateByKey(
      //buffer 存储缓冲区
      (seq: Seq[Int], buffer: Option[Long]) => {
        //跟新缓冲区
        println("#########################")
        println(seq.foreach(println))
        println("#########################")
        println(buffer.foreach(println))
        println("#########################")
        val result = seq.sum + buffer.getOrElse(0L)
        Option(result)
      }
    )
    stateDS.print()



    ssc.start()

    ssc.awaitTermination()
  }
}
