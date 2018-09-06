package com.mime.bdp.hbase

import org.apache.hadoop.hbase.{HRegionLocation, NamespaceDescriptor, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.Row
import java.nio.charset.StandardCharsets._

import collection.JavaConverters._
import HBaseConstants._
import com.mime.bdp.Logging
import org.apache.spark.sql.types._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.math.Ordering

class HBaseRDD(@transient sc: SparkContext,
               filters: Array[Filter],
               userSpecifiedschema: StructType,
               parameters: Map[String, String])
  extends RDD[Row](sc, Nil) with Logging {

  implicit val ordering: Ordering[HBaseType] = new Ordering[HBaseType] {

    def compare(x: HBaseType, y: HBaseType): Int = {
      return Bytes.compareTo(x, y)
    }
  }

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    val connection = getConnection.get

    val partition = split.asInstanceOf[HBaseScanPartition]

    val scan = createScan(partition.startAndEnd)

    val table = connection.getTable(getTableName)

    var result: mutable.ArrayBuffer[Row] = ArrayBuffer[Row]()

    try {
      val resultScanner = table.getScanner(scan)
      result = buildRows(resultScanner)
    } finally {
      table.close()
    }

    closeConnection
    result.toIterator
  }

  override protected def getPartitions: Array[Partition] = {

    implicit val connection: Connection = getConnection.get

    var index: Int = 0

    val (startOfRange, endOfRange) = getRange(filters)

    val startAndEndWithRegion: List[(StartAndEnd, HRegionLocation)] = getRegions

    val p = startAndEndWithRegion.flatMap {
      case ((start: HBaseType, end: HBaseType), _: HRegionLocation) => {


        if (ordering.compare(end, startOfRange) < 0 || ordering.compare(endOfRange, start) < 0)
          Array[Partition]()
        else {
          val result = Array[Partition](HBaseScanPartition(index, ((if (ordering.compare(startOfRange, start) > 0) startOfRange else start),
            (if (ordering.compare(end, endOfRange) > 0) endOfRange else end))))
          index = index + 1
          result
        }
      }
      case _ => Array[Partition]()
    }

    closeConnection
    p.toArray
  }

  def rangeCompare(a: HBaseType, b: HBaseType): Int = {
    ordering.compare(Option(a).getOrElse(Array[Byte](Byte.MinValue)), Option(b).getOrElse(Array[Byte](Byte.MinValue)))

  }

  def getTableName: TableName = TableName.valueOf(parameters.get(OPTION_NAMESPACE).getOrElse(NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR),
    parameters(OPTION_TABLE_NAME))

  private def getConnection: Option[Connection] = {
    HBaseConnectionPool.get(this.parameters.get(OPTION_USER_NAME))
  }

  private def closeConnection: Unit = {
    Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
      override def run(): Unit = HBaseConnectionPool.close()
    }))
  }

  private def getRegions(implicit connection: Connection): List[(StartAndEnd, HRegionLocation)] = {
    var regionLocatorOption: Option[RegionLocator] = None
    try {
      regionLocatorOption = Option[RegionLocator](connection.getRegionLocator(getTableName))

      val regionLocator = regionLocatorOption.get

      regionLocator.getStartKeys.zip(regionLocator.getEndKeys).zip(
        regionLocator.getAllRegionLocations.asScala).toList
    }
    finally {
      if (regionLocatorOption.isDefined) {
        regionLocatorOption.get.close()
      }
    }

  }

  private def getRange(filters: Array[Filter]): StartAndEnd = {

    val startOfRanges = mutable.ArrayBuffer[HBaseType]()
    val endOfRanges = mutable.ArrayBuffer[HBaseType]()

    filters.filter {
      filter =>
        filter.references.mkString("").startsWith(SCHEMA_ROWKEY) ||
          filter.references.mkString("").startsWith(SCHEMA_TIMESTAMP)
    }
      .foreach {
        case GreaterThan(_, value) => startOfRanges.append(value.toString.getBytes(UTF_8))
        case GreaterThanOrEqual(_, value) => startOfRanges.append(value.toString.getBytes(UTF_8))
        case LessThan(_, value) => endOfRanges.append(value.toString.getBytes(UTF_8))
        case LessThanOrEqual(_, value) => endOfRanges.append(value.toString.getBytes(UTF_8))
        case _ =>
      }

    (if (startOfRanges.isEmpty) Array[Byte]() else startOfRanges.sorted.head,
      if (endOfRanges.isEmpty) Array[Byte]() else endOfRanges.sorted.last)

  }

  private def createScan(startAndEnd: StartAndEnd): Scan = {
    val (start, end) = startAndEnd
    val s = new Scan()

    s.setStartRow(start)
    s.setStopRow(end)
    //    requiredColumns.foreach(t =>
    //      s.addColumn(parameters(OPTION_COLUMN_FAMILY_NAME).getBytes(UTF_8), t.getBytes(UTF_8))
    //    )
    s
  }

  private def buildRange(filers: Array[Filter]): StartAndEnd = {

    val rowKeyRange = filters.map(buildRange(_)).filter(_ != None).map(_.get).sorted.toSeq

    (rowKeyRange.head, rowKeyRange.last)
  }

  private def buildRange(filter: Filter): Option[HBaseType] = {

    filter match {
      case GreaterThan(attribute, value) => {
        println(value)
        None
      }

      case LessThan(attribute, value) => {
        println(value)
        None
      }

      case _ => None

    }

  }

  private def buildRows(resultScanner: ResultScanner): ArrayBuffer[Row] = {
    val rows = mutable.ArrayBuffer[Row]()
    var result: Result = null

    while ( {
      result = resultScanner.next
      result
    } != null) {
      rows.append(buildRow(result))
    }

    rows
  }

  val timestampReg = "^(\\d+)m.+$".r

  //  val groupIdReg = "^\\d+m([\\w\\W]+)$".r

  /**
    * 构建Spark Row
    *
    * @param result
    * @return
    */
  private def buildRow(result: Result): Row = {

    val rowKey = new String(result.getRow, UTF_8)

    val timestamp: Long = rowKey match {
      case timestampReg(s) => s.toLong
      case _ => result.getMap.asScala.head._2.asScala.head._2.asScala.head._1
    }

    val resultSeq = userSpecifiedschema.map {

      case StructField(SCHEMA_ROWKEY, StringType, _, _) => rowKey

      case StructField(SCHEMA_TIMESTAMP, LongType, _, _) => timestamp

      case StructField(SCHEMA_VALUE, valueType: DataType, _, _) => {

        valueType match {
          case StructType(cfs: Array[StructField]) => {
            Row.fromSeq(cfs.map {
              case StructField(familyFieldName, StructType(qualifiers: Array[StructField]), _, _) => {

                val familyName: HBaseType = familyFieldName.getBytes(UTF_8)

                Row.fromSeq(qualifiers.map {

                  case StructField(qualifierFieldName, ds: DataType, _, _) => {

                    val value = new String(result.getValue(familyName, qualifierFieldName.getBytes(UTF_8)), UTF_8)

                    ds match {

                      case LongType => value.toLong

                      case IntegerType => value.toInt

                      case _ => value
                    }
                  }
                })
              }
            })
          }
          case ByteType => result.current().getValueArray
        }
      }
    }

    Row.fromSeq(resultSeq)
  }

}

object HBaseRDD {

  def apply(sc: SparkContext, filter: Array[Filter], userSpecifiedschema: StructType, parameters: Map[String, String]): HBaseRDD =
    new HBaseRDD(sc, filter, userSpecifiedschema, parameters)

}

private[hbase] case class HBaseScanPartition(override val index: Int, startAndEnd: StartAndEnd) extends Partition
