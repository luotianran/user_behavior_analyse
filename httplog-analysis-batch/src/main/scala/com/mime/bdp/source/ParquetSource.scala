package com.mime.bdp.source

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  *  
 *  @author yao.huang
  *  @version 1.0  
  *  @date 2018/7/9 上午11:31  
  *  @project bdp-user-analysis
  *  */
object ParquetSource {

  def read(path: String, spark: SparkSession): DataFrame = {
    spark.read.parquet(path)
  }
}
