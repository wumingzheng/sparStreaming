package com.atguigu.bigdata.spark.projet.controller

import com.atguigu.bigdata.spark.projet.service.{HotAdvClickTopNService, HotAdvClickTopNService1}

class HotAdvClickTopNController {

  private val hotAdvClickTopNService = new HotAdvClickTopNService1

  def getRankDatasAnylysis(num: Int) = {
     hotAdvClickTopNService.getRankDatasAnylysis(num)
  }
}
