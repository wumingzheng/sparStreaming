package com.atguigu.spark.day02

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaCluster.Err
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object spark08_LowApi {
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

    val cluster: KafkaCluster = new KafkaCluster(kafkaParams)

    // TODO 根据主题获取分区
    //获取分区
    //通过topic获取可能取不到，所以要么是Err,要么是Set[TopicAndPartition]（要么是左要么是右）
    val prititionEither: Either[Err, Set[TopicAndPartition]] = cluster.getPartitions(Set(topic))

    // 准备了一个新的map集合，把每一个偏移量记录下来，然后就把这个map的数据更新，然后set,(可以理解为总的偏移量的集合)
    // 最终要返回的 Map
    var topicAndPartition2Long: Map[TopicAndPartition, Long] = Map[TopicAndPartition, Long]()

    if(prititionEither.isRight){
      val set: Set[TopicAndPartition] = prititionEither.right.get
      // TODO 获取指定topic的指定分区的偏移量
      //group消费者组为单位
      //某个topic的某个分区的偏移量（可以同时取多个topic分区的偏移量，一个消费者可以消费多个分区）
      //拿到topic分区不一定是这个消费者消费的，所以偏移量可能取不到
      val offsetEither: Either[Err, Map[TopicAndPartition, Long]] = cluster.getConsumerOffsets(group, set)
      if(offsetEither.isLeft){
        //这就说明根本没有消费到，这就意味着需要从头消费，偏移量从0开始
        //Map[TopicAndPartition, Long]  所以说Long就是偏移量，Long指定tipic的里面的所有偏移量从0开始
        // 遍历每个分区, 都从 0 开始消费

        // 就是把分区置0添加到map,
        //就是一个map数据增加的问题
        set.foreach {
          topicAndPartition => {
            //每个分区他的偏移量都是0，形成一个kv对，把每个topic的分区变成0
            //变成0之后放到上面这个总的map当中去
            //由于上面的map是一个不可变集合，+号是有问题的，（会产生新的集合），所以把新的map赋值给原来的map
            topicAndPartition2Long = topicAndPartition2Long + (topicAndPartition -> 0)
          }
        }
      }else{
        //这就说明可以拿到每一个偏移量
        //拿到以后，以后的更新在这个基础上加上更新的偏移量就可以，加上更新的再去set就可以了

        //拿到这个map正好跟上面的map是一样的，所及就跟上面的合并就行
        val map: Map[TopicAndPartition, Long] = offsetEither.right.get

        val map1 = topicAndPartition2Long
        val map2 = map
        //这两个map进行合并操作
        map1.foldLeft(map2)(
          (innerMap,kv) => {
            val k = kv._1
            val v = kv._2

            val newValue: Long = innerMap.getOrElse(k,0L) + v

            innerMap + (k -> newValue)
          }
        )
      }
    }

    val dStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, String](
      ssc,
      kafkaParams,
      topicAndPartition2Long,
      (message: MessageAndMetadata[String, String]) => message.message()
    )
    dStream.print()
    // 保存 offset
//    saveOffset(cluster, group, dStream)
    dStream.foreachRDD(
      rdd => {
//        // 把 RDD 转换成HasOffsetRanges对
//
//        //获取当前rdd消费的进度
//        //更新map集合中每一个分区的offset
//        topicAndPartition2Long
//        //更新集群中的分区offset
//        cluster.setConsumerOffsets(group,topicAndPartition2Long)

        var map: Map[TopicAndPartition, Long] = Map[TopicAndPartition, Long]()
        // 把 RDD 转换成HasOffsetRanges对
        val hasOffsetRangs: HasOffsetRanges = rdd.asInstanceOf[HasOffsetRanges]
        // 得到 offsetRangs
        val ranges: Array[OffsetRange] = hasOffsetRangs.offsetRanges
        ranges.foreach(range => {
          // 每个分区的最新的 offset
          map += range.topicAndPartition() -> range.untilOffset
        })
        cluster.setConsumerOffsets(group,map)
      }
    )
    ssc.start()
    ssc.awaitTermination()

  }
}
