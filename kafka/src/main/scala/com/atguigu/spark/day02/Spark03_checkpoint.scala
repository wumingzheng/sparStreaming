package com.atguigu.spark.day02

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Spark03_checkpoint {
  def main(args: Array[String]): Unit = {

    val sc: StreamingContext = StreamingContext.getActiveOrCreate("E:\\MyWork\\WorkSpace\\sparStreaming\\kafka\\input", () => {
      val conf: SparkConf = new SparkConf().setAppName("checkpoint").setMaster("local[*]")
      val ssc = new StreamingContext(conf, Seconds(3))
      // TODO有状态数据的保存是放置在检查点中，所以需要设定检查点路径
      ssc.checkpoint("E:\\MyWork\\WorkSpace\\sparStreaming\\kafka\\input")

      val dstream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",9999)

      dstream.print()

      val wordDS: DStream[String] = dstream.flatMap(_.split(" "))

      val mapDS: DStream[(String, Int)] = wordDS.map(
        word => {
          (word, 1)
        }
      )

      val stateDS: DStream[(String, Long)] = mapDS.updateStateByKey(
        //buffer 存储缓冲区
        (seq: Seq[Int], buffer: Option[Long]) => {
          //跟新缓冲区
          val result = seq.sum + buffer.getOrElse(0L)
          Option(result)
        }
      )
      stateDS.print()


      ssc
    })


    sc.start()
    sc.awaitTermination()
  }
}
