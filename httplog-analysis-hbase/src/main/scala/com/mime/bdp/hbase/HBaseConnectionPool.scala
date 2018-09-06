package com.mime.bdp.hbase

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}

import scala.collection.mutable

object HBaseConnectionPool {

  val connectionMap = mutable.HashMap[Option[String], Connection]()

  def get(key: Option[String], hbaseConf: Configuration = HBaseConfiguration.create()): Option[Connection] = {
    Option(connectionMap.getOrElseUpdate(key, ConnectionFactory.createConnection(hbaseConf)))
  }

  def close(): Unit = {
    this.connectionMap.values.filter(!_.isClosed).foreach(IOUtils.closeQuietly(_))
    this.connectionMap.clear()
  }
}

