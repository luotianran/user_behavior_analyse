package com.mime.bdp

import com.databricks.spark.avro.SchemaConverters
import com.mime.bdp.config.Constants.{KEY_EVENT_NAME, KEY_EVENT_TIME, KEY_GROUP_BY}
import com.mime.bdp.job.SparkJob
import org.apache.avro.Schema
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DataType, StringType, TimestampType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

/**
  *  
  * @author  yao.huang
  * @version  1.0 
  */
object Bootstrap extends SparkJob("httplog-analysis-streaming") with Logging {

  import spark.implicits._

  import com.mime.bdp.source.kafkaSourceCreator._
  import com.mime.bdp.config.CmdArgsParser._
  import com.mime.bdp.udf.streamingJobUdfCreator._

  def main(args: Array[String]): Unit = {
    val config = parse(args)
    val broker = config.broker
    val topics = config.topics
    val schemaString = config.schema
    val log = config.log
//    val eventList = config.events.split(",").map(uri => uri.trim)
    val eventName = config.eventName
    val eventTime = config.eventTime
    val groupBy = config.groupBy

    val tableName = config.tables

    val allLogLevel = Array("ALL", "DEBUG", "ERROR", "FATAL", "INFO", "OFF", "TRACE", "WARN")

    spark.sparkContext.setLogLevel(allLogLevel.find(_.equalsIgnoreCase(log)).getOrElse("DEBUG"))

    val kafkaProp = Map(
      s"${KAFKA_PREFIX}${ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG}" -> broker,
      "subscribe" -> topics
    )

//    val eventNameFilterUdf = createEventFilterUdf(eventList)
//    .filter(eventNameFilterUdf($"${eventName}"))

    val streaming = decodeAvroMessage(createStream(kafkaProp), schemaString).
      select($"value.*").
      select(
        $"${groupBy}" as s"${KEY_GROUP_BY}",
        $"${eventName}" as s"${KEY_EVENT_NAME}",
        $"${eventTime}".cast(TimestampType).as(s"${KEY_EVENT_TIME}")
      ).withColumn("ds", date_format($"${KEY_EVENT_TIME}", "yMMdd"))
      .groupBy($"${KEY_GROUP_BY}", $"ds")
      .agg(collect_list(
        struct($"${KEY_EVENT_NAME}",
          $"${KEY_EVENT_TIME}")
      ).as("info"))

    val query = streaming.writeStream
      .format("com.mime.bdp.sink.HBaseSinkProvider")
      .option("checkpointLocation", "/tmp/hbase-streaming")
      .option("cf", "info")
      .option("table", tableName)
      .trigger(Trigger.ProcessingTime("2 minutes"))

      //      .outputMode("append")
      .start()

    Runtime.getRuntime.addShutdownHook(new Thread("shutdown hook") {
      override def run(): Unit = {
        // do sth
      }
    })

    query.awaitTermination()
  }

  def decodeAvroMessage(stream: DataFrame, schemaString: String): DataFrame = {
    val avroValueDecodeUdf = createAvroTypeUdf(schemaString)
    val sparkSchema = convertAvroSchema2SparkSchema(schemaString)
    stream.withColumn("key", $"key".cast(StringType)).withColumn("value", from_json(avroValueDecodeUdf($"value"), sparkSchema))
  }

  def convertAvroSchema2SparkSchema(schemaString: String): DataType = {
    val schema = new Schema.Parser().parse(schemaString)
    SchemaConverters.toSqlType(schema).dataType
  }

}

