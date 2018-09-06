package com.mime.bdp.hbase

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources._
import org.apache.spark.sql.streaming.OutputMode

class DefaultSource extends StreamSinkProvider with RelationProvider
  with DataSourceRegister {

  //
  val hdfsPattern = "hdfs://".r

  override def createSink(sqlContext: SQLContext,
                          parameters: Map[String, String],
                          partitionColumns: Seq[String],
                          outputMode: OutputMode): Sink = {

    new HBaseRelation(sqlContext, parameters)
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {

    new HBaseRelation(sqlContext, parameters)
  }

  override def shortName(): String = "hbase"
}
