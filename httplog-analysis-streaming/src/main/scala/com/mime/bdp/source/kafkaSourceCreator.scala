package com.mime.bdp.source

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  *  
 *  @author yao.huang
  *  @version 1.0  
  *  @date 2018/7/12 下午5:35  
  *  @project bdp-user-analysis
  *  */
object kafkaSourceCreator {

  val KAFKA_PREFIX = "kafka."

  def createStream(props: Map[String, String]) (implicit spark: SparkSession): DataFrame = {
   spark.readStream.format("kafka").options(props).load()
  }

}
