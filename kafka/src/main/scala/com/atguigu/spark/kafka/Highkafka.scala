package com.atguigu.spark.kafka

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Highkafka {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Higekafka").setMaster("local[*]")
    var ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    //kafka参数声明
    val brokers = "hadoop102:9092,hadoop103:9092,hadoop104:9092"
    val topic ="bigdata"
    val group = "bigdata"
    val deserialization = "org.apache.kafka.common.serialization.StringDeserializer"
    val kafkaParams = Map(
      ConsumerConfig.GROUP_ID_CONFIG -> group,
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> deserialization,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> deserialization
    )

    var dStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc,
      kafkaParams,
      Set(topic)
    )

    dStream.print()

    ssc.start()

    ssc.awaitTermination()

  }
}
