package com.atguigu.bigdata.spark.projet.common

import com.atguigu.bigdata.spark.projet.util.SparkStreamingEnv
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils

trait TDao {

  //读取kafka数据
  def readKafkaData():DStream[String] = {
    //kafka参数声明
    val brokers = "hadoop102:9092,hadoop103:9092,hadoop104:9092"
    val topic = "bigdata"
    val group = "bigdata"//是以消费者组为单位的
    val deserialization = "org.apache.kafka.common.serialization.StringDeserializer"
    val kafkaParams = Map(
      ConsumerConfig.GROUP_ID_CONFIG -> group,
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> deserialization,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> deserialization
    )

    val kafkaDS: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      SparkStreamingEnv.get(),
      kafkaParams,
      Set(topic)
    )
    kafkaDS.map(_._2)
  }

  def readScoketData():DStream[String] = {
    null
  }
}
