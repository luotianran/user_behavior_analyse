package com.mime.bdp.hbase

import com.mime.bdp.Logging
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.sql.{DataFrame, SparkSession}

private[hbase] object HBaseWriter extends Logging {

  val conf = HBaseConfiguration.create()

  private def vaildParameter(parameter: Map[String, String]) = {
    // 参数验证

  }

  def write(sparkSession: SparkSession,
            df: DataFrame,
            hbaseParameters: Map[String, String]) = {

    vaildParameter(hbaseParameters)

    df.rdd.foreachPartition {
      rows =>
        val task = new HBaseWriteTask(hbaseParameters)
        task.execute(rows)
    }

  }
}


