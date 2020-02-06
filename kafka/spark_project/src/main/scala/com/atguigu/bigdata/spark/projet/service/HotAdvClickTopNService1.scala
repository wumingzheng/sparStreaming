package com.atguigu.bigdata.spark.projet.service

import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.bigdata.spark.projet.dao.HotAdvClickTopNDao
import org.apache.spark.streaming.Minutes
import org.apache.spark.streaming.dstream.DStream

class HotAdvClickTopNService1 {
  // Data Access Object
  // 只做数据访问
  private val hotAdvClickTopNDao = new HotAdvClickTopNDao

  def getRankDatasAnylysis(num: Int) = {

    val kafkaDS: DStream[String] = hotAdvClickTopNDao.readKafkaData()

    // TODO 逻辑处理
    // 每天每地区热门广告Top3

    //TODO 1，将获取数据进行结构转换
    val advDS: DStream[(String, Int)] = kafkaDS.map(
      line => {
        val datas: Array[String] = line.split(",")

        val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm")

        (sdf.format(new Date(datas(0).toLong)) + "_" + datas(1) + "_" + datas(4), 1)
      }
    )

    advDS.window(Minutes(60),Minutes(10))

    val groupDS: DStream[(String, Iterable[(String, Int)])] = advDS.map(
      line => {
        val ts: Array[String] = line._1.split("_")
        (ts(2), (ts(0), line._2))
      }
    ).groupByKey()

    val value: DStream[(String, String)] = groupDS.map {
      case (id, v) => {
//        (id, v.groupBy(_._1).toList.reduce(_ + _))
      }
    }
    value



  }
}
