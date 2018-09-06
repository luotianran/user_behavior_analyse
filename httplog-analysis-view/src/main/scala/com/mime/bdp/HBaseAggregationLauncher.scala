package com.mime.bdp

import com.mime.bdp.job.SparkJob

object HBaseAggregationLauncher extends SparkJob("hbase-aggregation") with Logging {

  spark.read.text()

}
