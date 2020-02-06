package com.atguigu.bigdata.spark.projet.common

import com.atguigu.bigdata.spark.projet.util.SparkStreamingEnv
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

trait TApplication {
  def start(appName: String = "application", duration: Int = 3)(op: => Unit) = {
    // TODO 1. Spark环境
    SparkStreamingEnv.init(appName,duration)

    // TODO 2. 处理
    try {
      op
    } catch {
      case e => e.printStackTrace()
    }

    // TODO 3. 启动 & 等待
    SparkStreamingEnv.execute()


    this
  }

  def stop(op: => Unit) = {
      //优雅的关闭
    op
  }
}
