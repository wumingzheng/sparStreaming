package com.atguigu.spark.demo

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkSourceDIY {
  def main(args: Array[String]): Unit = {
    // TODO 使用SparkStreaming完成wordCount的操作

    // TODO 创建上下文对象
    var conf: SparkConf = new SparkConf().setAppName("sourceDIY").setMaster("local[*]")
    var ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    // TODO 2. 对数据进行处理
    // TODO 2.1 采集数据
    val receiverDS: ReceiverInputDStream[String] = ssc.receiverStream(new MyReceiver("hadoop102", 9999))
    // TODO 2.2 将采集数据进行扁平化操作
    val wordDS: DStream[String] = receiverDS.flatMap( _.split(" ") )
    // TODO 2.3 将扁平化后的数据进行结构的转换
    val wordToOneDS: DStream[(String, Int)] = wordDS.map(word => (word, 1))
    // TODO 2.4 将转换结构后的数据进行聚合处理
    val wordToCountDS: DStream[(String, Int)] = wordToOneDS.reduceByKey(_+_)

    // TODO 2.5 将结果打印出来
    wordToCountDS.print()

    // TODO 3. 启动数据采集器
    ssc.start()

    // TODO 4. 让driver等带采集器的结束
    ssc.awaitTermination()

    // TODO 5. 关闭环境对象

  }
}
// 自定义数据采集器
// 1. 重写Receiver类
// 2. 重写方法onStart, onStop

class MyReceiver(host:String,port:Int) extends Receiver[String](StorageLevel.MEMORY_ONLY){

  private var socket:Socket = _

  override def onStart(): Unit = {
    new Thread(
      new Runnable {
        override def run(): Unit = {
          socket = new Socket(host, port)
          val reader = new BufferedReader(
            new InputStreamReader(
              socket.getInputStream,
              "UTF-8"
            )
          )
          var line = ""
          while ( (line = reader.readLine()) != null ){
            store(line)
          }
        }
      }
    ).start()
  }

  override def onStop(): Unit = {
    if(socket != null){
      socket.close()
    }
  }
}
