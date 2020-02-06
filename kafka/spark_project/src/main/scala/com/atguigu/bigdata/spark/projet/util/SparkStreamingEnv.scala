package com.atguigu.bigdata.spark.projet.util

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamingEnv {

  private val local: ThreadLocal[StreamingContext] = new ThreadLocal[StreamingContext]

  def init(name:String,dur:Int) = {
    val sparkConf: SparkConf = new SparkConf().setAppName(name).setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(dur))
    ssc.sparkContext.setCheckpointDir("E:\\MyWork\\WorkSpace\\sparStreaming\\kafka\\input")
    local.set(ssc)
  }

  def execute() = {
    local.get().start()
    local.get().awaitTermination()
  }

  def get() = {
    local.get()
  }
}
