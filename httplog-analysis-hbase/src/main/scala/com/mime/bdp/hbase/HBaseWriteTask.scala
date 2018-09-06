package com.mime.bdp.hbase

import java.sql.Timestamp

import com.mime.bdp.Logging
import com.mime.bdp.config.Constants._
import com.mime.bdp.hbase.HBaseConstants._
import org.apache.commons.io.Charsets._
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.{HBaseConfiguration, NamespaceDescriptor, TableName}
import org.apache.spark.sql.Row

import scala.collection.JavaConverters._


private[hbase] class HBaseWriteTask(parameter: Map[String, String]) extends Logging {

  val conf = HBaseConfiguration.create()

  def execute(rows: Iterator[Row]) = {
    var hbaseConnection: Connection = null
    var admin: Admin = null
    try {
      hbaseConnection = ConnectionFactory.createConnection(conf)
      admin = hbaseConnection.getAdmin
      // TODO 验证表
//      admin.tableExists(tableName)

      val tableName = TableName.valueOf(parameter.get(OPTION_NAMESPACE).getOrElse(NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR), parameter(OPTION_TABLE_NAME))

      val table = hbaseConnection.getTable(tableName)

      val batch = createHbaseBatch(rows)

      val result = new Array[AnyRef](batch.size)

      // TODO 结果验证,加入回调, 异常记录
      table.batch(batch.asJava, result)

    } finally {

      if (admin != null)
        admin.close()

      if (hbaseConnection != null)
        hbaseConnection.close()
    }

  }

  def createHbaseBatch(rows: Iterator[Row]): List[Put] = {
    val cf = parameter.get(OPTION_COLUMN_FAMILY_NAME).getOrElse("info")

    rows.flatMap {
      row =>
        val timestamp = row.getAs[Timestamp](KEY_EVENT_TIME).getTime.toString
        val groupBy = "m" + row.getAs[String](KEY_GROUP_BY)

        val rowKey1 = groupBy + timestamp
        val put1 = createPut(rowKey1, cf, row)

        val rowkey2 = timestamp + groupBy
        val put2 = createPut(rowkey2, cf, row)

        Array(put1, put2)

    }.toList

  }

  def createPut(rowKey: String, cf: String, row: Row): Put = {
    val put = new Put(rowKey.getBytes(UTF_8))
    val eventTime = row.getAs[Timestamp](KEY_EVENT_TIME).getTime.toString
    val eventName = row.getAs[String](KEY_EVENT_NAME)

    put.addColumn(cf.getBytes(UTF_8), KEY_EVENT_TIME.getBytes(UTF_8), eventTime.getBytes(UTF_8))
    put.addColumn(cf.getBytes(UTF_8), KEY_EVENT_NAME.getBytes(UTF_8), eventName.getBytes(UTF_8))

    put
  }
}
