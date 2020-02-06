package com.atguigu.spark.demo

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object HigKafka {
  def main(args: Array[String]): Unit = {
    //创建上下文对象
    var conf: SparkConf = new SparkConf().setAppName("HigKafka").setMaster("local[*]")

    var ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    // kafka 参数
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

    val dStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc,
      kafkaParams,
      Set(topic)
    )
    dStream.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
