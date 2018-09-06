package com.mime.bdp.hbase

import com.mime.bdp.Logging
import com.mime.bdp.hbase.HBaseConstants._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan}
import org.apache.spark.sql.types._

class HBaseRelation(val _sqlContext: SQLContext, parameter: Map[String, String]) extends
  BaseRelation with PrunedFilteredScan with Sink with Logging {

  override def sqlContext: SQLContext = _sqlContext

  override def schema: StructType = createSchema(parameter.get("schema"))

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    createHBaseRDD(requiredColumns, filters)
  }

  private def createHBaseRDD(requiredColumns: Array[String], filters: Array[Filter]): HBaseRDD =
    HBaseRDD(sqlContext.sparkContext, filters, this.pruneSchema(this.schema, requiredColumns), parameter)

  private def pruneSchema(schema: StructType, columns: Array[String]): StructType = {
    val fieldMap = Map(schema.fields.map(x => x.name -> x): _*)
    new StructType(columns.map(name => fieldMap(name)))
  }

  @volatile private var latestBatchId = -1L

  override def addBatch(batchId: Long, data: DataFrame): Unit = {

    if (batchId <= latestBatchId) {
      info(s"Skipping already committed batch $batchId")
    } else {
      HBaseWriter.write(sqlContext.sparkSession, data, parameter)

      latestBatchId = batchId
    }
  }

  val baseSchema = new StructType().add(SCHEMA_ROWKEY, StringType).add(SCHEMA_TIMESTAMP, LongType)

  private def createSchema(schemaOption: Option[String]): StructType = schemaOption match {
    // TODO schema生成逻辑
    case Some(schemaStr) => baseSchema.add(SCHEMA_VALUE, DataType.fromJson(schemaStr))
    case None => baseSchema.add(SCHEMA_VALUE, ByteType)
  }

}

