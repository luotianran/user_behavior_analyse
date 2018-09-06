package com.mime.bdp.job

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  *  
 *  @author yao.huang
  *  @version 1.0  
  *  @date 2018/7/12 下午5:27  
  *  @project bdp-user-analysis
  *  */
abstract class SparkJob(appName: String, conf: SparkConf = new SparkConf()) {

  implicit lazy val spark = SparkSession.builder().appName(appName).config(conf).getOrCreate()

}
