package com.atguigu.bigdata.spark.projet.application

import com.atguigu.bigdata.spark.projet.common.TApplication
import com.atguigu.bigdata.spark.projet.controller.HotAdvClickTopNController

object HotAdvClickTopNApplication extends App with TApplication {

  // TODO 启动应用程序
  start("HotAdvClick",5){
    // TODO 业务数据

    val hotAdvClickTopNController = new HotAdvClickTopNController

    hotAdvClickTopNController.getRankDatasAnylysis(3).print

  } stop {
    // TODO 关闭逻辑
  }
}
