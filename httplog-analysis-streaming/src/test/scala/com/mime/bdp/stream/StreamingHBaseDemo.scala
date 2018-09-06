package com.mime.bdp.stream

import com.mime.bdp.config.CmdArgsParser.parse
import com.mime.bdp.config.Constants.{KEY_EVENT_NAME, KEY_EVENT_TIME, KEY_GROUP_BY}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{SQLContext, SQLImplicits, SparkSession}
import org.scalatest.{BeforeAndAfter, FunSuite}

class StreamingHBaseDemo extends FunSuite with BeforeAndAfter {
  t =>

  var spark: SparkSession = _

  before {

    spark = SparkSession.builder()
      .master("local[2]")
      .appName("streaming tests")
      .getOrCreate()

  }

  object sparkImplicit_ extends SQLImplicits {
    override protected def _sqlContext: SQLContext = t.spark.sqlContext
  }

  import sparkImplicit_._

  test("test hbase") {
    implicit val _spark = spark
    import com.mime.bdp.Bootstrap._
    import com.mime.bdp.source.kafkaSourceCreator._

    val args = Array("--config=/Users/clover/Desktop/work_mime/bigdata/bdp-user-analysis/httplog-analysis-streaming/src/test/resources/config.properties")
    val config = parse(args)

    val schema = config.schema

    val kafkaProp = Map(
      //      s"${KAFKA_PREFIX}${ConsumerConfig.GROUP_ID_CONFIG}" -> "test-group-01",
      s"${KAFKA_PREFIX}${ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG}" -> config.broker,
      //      s"${KAFKA_PREFIX}avro.schema" -> schema,
      //      s"${KAFKA_PREFIX}${ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG}" -> "com.mime.bdp.source.KafkaAvroDeserializer",
      //      s"${KAFKA_PREFIX}${ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG}" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "subscribe" -> config.topics
    )

    val groupBy = config.groupBy
    val eventName = config.eventName
    val eventTime = config.eventTime

    val streamingDf = decodeAvroMessage(
      createStream(kafkaProp)
      , schema
    ).select($"value.*")
      .where(s"${groupBy} is not null")
      .select(
        $"${groupBy}" as s"${KEY_GROUP_BY}",
        $"${eventName}" as s"${KEY_EVENT_NAME}",
        $"${eventTime}".cast(TimestampType).as(s"${KEY_EVENT_TIME}")
      )
    //      .withColumn("ds", date_format($"${KEY_EVENT_TIME}", "yMMdd"))
    //      .groupBy($"${KEY_GROUP_BY}", $"ds")
    //      .agg(collect_list(
    //        struct($"${KEY_EVENT_NAME}",
    //          $"${KEY_EVENT_TIME}")
    //      ).as("info"))

    val query = streamingDf.writeStream.
      //      format("console")
      //      .option("truncate", "false")
      //      .trigger(Trigger.ProcessingTime("5 seconds"))
      //      .start()


      outputMode("append").
      format("com.mime.bdp.hbase").
      option("checkpointLocation", "/tmp/hbase-streaming").
      option("cf", "info").
      option("table", "user_events").
      //      trigger(Trigger.ProcessingTime("2 minutes")).
      trigger(Trigger.ProcessingTime("5 seconds")).
      start()

    query.awaitTermination()

  }

  test("hbase source") {

    val df = spark.read.format("com.mime.bdp.hbase")
      .option("schema", """{"type":"struct","fields":[{"name":"info","type":{"type":"struct","fields":[{"name":"event_name","type":"string","nullable":true,"metadata":{}}]},"nullable":true,"metadata":{}}]}""")
      .option("table", "user_events")
      .load()

    df.where("rowkey > '1534476880000' and rowkey < '1534476880001'").show(false)
//    df.show(false)

  }

}
