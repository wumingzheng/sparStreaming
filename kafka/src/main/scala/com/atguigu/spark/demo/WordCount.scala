package com.atguigu.spark.demo

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    //使用SparkStreamCount完成WordCount的操作

    // TODO 1，创建上下文环境对象
    //SparkStreaming本地模式时不能使用单线程操作
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val ssc = new StreamingContext(sparkConf,Seconds(3))

    //上下文环境对象可以根据实际的应用场景创建数据采集器

    // TODO 2.对数据进行处理
    // TODO 2.1 采集数据
    //    var socketDS: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)
    val socketDS: DStream[String] = ssc.textFileStream("E:\\MyWork\\WorkSpace\\sparStreaming\\kafka\\input")

    // TODO 2.2 将采集到的数据进行扁平化
    val wordDS: DStream[String] = socketDS.flatMap(_.split(" "))

    // TODO 2.3 将扁平化后的数据进行结构转换
    val wordToOneDS: DStream[(String, Int)] = wordDS.map(word => (word, 1))

    // TODO 2.4 将转换结构后的数据进行聚合处理
    val wordToCountDS: DStream[(String, Int)] = wordToOneDS.reduceByKey(_ + _)

    // TODO 2.5 将结果打印出来
    wordToCountDS.print()

    // TODO 3. 启动数据采集器(采集器是长期独立的运行的，所以单独启动)
    ssc.start()

    // TODO 4. 让Driver等待采集器结束(采集器启动以后driver不能停，要让driver阻塞，等待采集器的结束，采集器不结束driver不能停)
    ssc.awaitTermination()

    // TODO 5. 关闭环境对象
    //ssc.stop()
    // 优雅的关闭
    //ssc.stop()

  }
}
