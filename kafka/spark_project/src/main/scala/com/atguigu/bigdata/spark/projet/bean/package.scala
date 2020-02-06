package com.atguigu.bigdata.spark.projet

import java.sql.Timestamp

package object bean {
  case class HotAdvClick(adv:Int,click:Long)
  case class AdsInfo(ts: Long,
//                     timestamp: Timestamp,
//                     dayString: String,
//                     hmString: String,
                     area: String,
                     city: String,
                     userId: String,
                     adsId: String)
}
