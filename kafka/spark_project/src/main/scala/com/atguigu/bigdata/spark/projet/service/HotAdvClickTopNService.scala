package com.atguigu.bigdata.spark.projet.service

import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.bigdata.spark.projet.bean
import com.atguigu.bigdata.spark.projet.dao.HotAdvClickTopNDao
import com.atguigu.bigdata.spark.projet.moke.RedisUtil
import org.apache.spark.streaming.dstream.DStream
import org.json4s.native.JsonMethods
import redis.clients.jedis.Jedis

class HotAdvClickTopNService {
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

        val sdf = new SimpleDateFormat("yyyy-MM-dd")

        (sdf.format(new Date(datas(0).toLong)) + "_" + datas(1) + "_" + datas(4), 1)
      }
    )

    // TODO 2，使用有状态操作完成数据的聚合处理
    val stateDS: DStream[(String, Long)] = advDS.updateStateByKey(
      (seq: Seq[Int], buffer: Option[Long]) => {
        val sum: Long = seq.sum + buffer.getOrElse(0L)
        Option(sum)
      }
    )

    // TODO 3，间聚合的结果进行结构的转换 （ts_area_adv, sum）=> (ts,(area_adv,sum))
    // TODO 3.1 将转换后的数据根据时间进行分组
    val tsGroupDS: DStream[(String, Iterable[(String, Long)])] = stateDS.map {
      case (k, sum) => {
        val ks: Array[String] = k.split("_")
        (ks(0), (ks(1) + "_" + ks(2), sum))
      }
    }.groupByKey()

    // TODO 4，将分组后的数据对区域进行分组处理
    val resultDS: DStream[(String, Map[String, List[(String, Long)]])] = tsGroupDS.mapValues(
      list => {
        val areaGroupDS: Map[String, Iterable[(String, (String, Long))]] = list.map {
          case (k, sum) => {
            val ks: Array[String] = k.split("_")
            (ks(0), (ks(1), sum))
          }
        }.groupBy(_._1)

        // TODO 4.1 将分组后的数据（adv,sum）进行排序，取前n个
        areaGroupDS.mapValues(
          mapList => {
            val advToCountList: List[(String, Long)] = mapList.map(_._2).toList
            advToCountList.sortWith(
              (l, r) => {
                l._2 > r._2
              }
            )
          }
        ).take(num)
      }
    )
    resultDS

    // 5，将结果数据进行遍历，保存到redis中
//    resultDS.foreachRDD(
//      rdd => {
//        rdd.foreach(
//          data => {
//            val ts: String = data._1
//            val data1: Map[String, List[(String, Long)]] = data._2
//            val client: Jedis = RedisUtil.getJedisClient
//            data1.foreach(
//              data11 => {
//                import org.json4s.JsonDSL._
//                // TODO 5.1，向redis中保存数据因该是字符串类型
//                val adsCountJsonString = JsonMethods.compact(JsonMethods.render(List(data)))
//                client.hset("area:abs:top3" + ts, data11._1, adsCountJsonString)
//              }
//            )
//
//          }
//        )
//      }
//    )

  }
}
