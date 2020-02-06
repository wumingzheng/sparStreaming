package com.atguigu.spark.day02

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

object spark07_stop {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("window").setMaster("local[*]")
    conf.set("spark.streaming.stopGracefullyOnShutdown", "true")

    val ssc = new StreamingContext(conf,Seconds(3))

    val socketDS: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",9999)

    val resultDS: DStream[(String, Int)] = socketDS.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    resultDS.print()

    new Thread(new Runnable {
      override def run(): Unit = {

        while ( true ) {
          try {
            Thread.sleep(5000)
          } catch {
            case ex : Exception => println(ex)
          }

          // 监控HDFS文件的变化
          val fs: FileSystem = FileSystem.get(new URI("hdfs://hadoop102:9000"), new Configuration(), "root")

          //获取当前sparkStreaming的状态
          val state: StreamingContextState = ssc.getState()
          // 如果环境对象处于活动状态，可以进行关闭操作
          if ( state == StreamingContextState.ACTIVE ) {
            // 判断路径是否存在
            val flg: Boolean = fs.exists(new Path("hdfs://hadoop102:9000/stopSpark2"))
            if ( flg ) {
              ssc.stop(true, true)
              System.exit(0)
            }

          }
        }

      }
    }).start()

    ssc.start()
    ssc.awaitTermination()

  }
}