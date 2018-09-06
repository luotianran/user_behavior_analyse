package com.mime.bdp


import java.util.Date

import com.mime.bdp.config.Constants._
import com.mime.bdp.config.CmdArgsParser._
import com.mime.bdp.fsm._
import com.mime.bdp.job.SparkJob
import com.mime.bdp.udf.batchJobUdfCreator._
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer

/**
  *  
 *  @author yao.huang
  *  @version 1.0  
  *  @date 2018/7/9 上午10:35  
  *  @project bdp-user-analysis
  *  */
object Bootstrap extends SparkJob("httplog-analysis-batch") with Logging {

  import source.ParquetSource._
  import spark.implicits._

  def main(args: Array[String]): Unit = {

    val cmdArg = parse(args)

    val inputPath = cmdArg.input
    val outputPath = cmdArg.output
    val eventList = cmdArg.events.split(",").map(uri => uri.trim)
    val eventName = cmdArg.eventName
    val eventTime = cmdArg.eventTime
    val groupBy = cmdArg.groupBy

    val tableNames = getOutputTableNames(cmdArg.resultTables)

    require(tableNames != None, "tables参数输入有误")

    val (table1, table2) = tableNames.get

    require(!(table1.isEmpty || table2.isEmpty), "Table name should not be empty")


    val uriStateMap = eventList.zipWithIndex.map {
      case (uri: String, i: Int) => uri -> s"e${i}"
    }.toMap

    lazy val transactions = for (i <- 0 until eventList.size) yield {
      val eventType = eventList(i)
      val (sourceState, targetState) = if (i == 0) InitailState -> State(eventType) else State(eventList(i - 1)) -> State(eventType)
      new TransactionImpl[DefaultEvent](sourceState, targetState, eventType, null)
    }

    lazy val states = transactions.flatMap(t => Seq(t.getSourceState, t.getTargetState)).toSet.toSeq

    val df = read(inputPath, spark)

    val filterudf = createEventFilterUdf(uriStateMap.keySet.toSeq)

    val window = Window.partitionBy($"${groupBy}").orderBy($"${eventTime}")

    val partitionRank = rank().over(window)

    val httplogEventDf = df.filter(filterudf($"${eventName}")).
      select($"${groupBy}" as s"${KEY_GROUP_BY}",
        $"${eventName}" as s"${KEY_EVENT_NAME}",
        $"${eventTime}".cast(TimestampType).as(s"${KEY_EVENT_TIME}"),
        partitionRank.as("rank")
      ).withColumn("ds", date_format($"${KEY_EVENT_TIME}", "y-MM-dd")).
      groupBy($"${KEY_GROUP_BY}", $"ds").
      agg(collect_list(
        struct($"${KEY_EVENT_NAME}",
          $"${KEY_EVENT_TIME}",
          $"rank")
      ).as("info")).cache()

    val repartitionNum = spark.conf.getOption("spark.executor.instances").getOrElse("1").toInt

    httplogEventDf.repartition(repartitionNum).write.mode(SaveMode.Append).partitionBy("ds").parquet(s"${outputPath}/${table1}")

    val aggState = (rows: Seq[Row]) => {
      val stateInfos = rows.sortBy(f => f.getAs[Int]("rank"))

      val fsm = new FSMImpl(states, transactions)
      val result = ArrayBuffer[State]()

      var prevState: State = InitailState
      var currState: State = InitailState

      stateInfos.foreach { row =>
        val timestamp = row.getAs[Date](s"${KEY_EVENT_TIME}").getTime
        val e = DefaultEvent(row.getAs[String](s"${KEY_EVENT_NAME}"), timestamp)

        fsm.emitEvent(e)

        currState = fsm.currentState
        if (currState != prevState) {
          result append currState
          prevState = currState
        }
      }

      result.map(_.name)
    }

    val aggStateUdf = org.apache.spark.sql.functions.udf(aggState)

    httplogEventDf.select($"$KEY_GROUP_BY", $"ds", aggStateUdf($"info").as("states")).where(size($"states") > 0).
      repartition(repartitionNum).
      write.mode(SaveMode.Append).partitionBy("ds").parquet(s"${outputPath}/${table2}")

  }

  def getOutputTableNames(tables: String): Option[(String, String)] = {

    tables.split(",")
      .map(_.trim).toList match {
      case t :: Nil => Some(t -> t)
      case t1 :: t2 :: _ => Some(t1 -> t2)
      case _ => None
    }
  }

}

