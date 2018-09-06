package com.mime.bdp.stream

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.sql.{SQLContext, SQLImplicits, SparkSession}
import org.scalatest.{BeforeAndAfter, FunSuite}

class StreamingTests extends FunSuite with BeforeAndAfter {
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

  test("test1") {
    implicit val _spark = spark
    import com.mime.bdp.source.kafkaSourceCreator._
    import com.mime.bdp.Bootstrap._

    val schema = """{"namespace":"cn.mime.bdp.test.beans","type":"record","name":"httplog_basis","doc":"basisfields","fields":[{"name":"recordid","type":["null","string"]},{"name":"mtraceid","type":["null","string"]},{"name":"clientid","type":["null","string"]},{"name":"requestid","type":["null","string"]},{"name":"requesturi","type":["null","string"]},{"name":"requestip","type":["null","string"]},{"name":"requestts","type":["null","string"]},{"name":"responsetime","type":["null","string"]},{"name":"requestheaders","type":{"name":"requestheaders","type":"record","doc":"requestheadersbasisfields","fields":[{"name":"servicetoken","type":["null","string"]},{"name":"documentNo","type":["null","string"]},{"name":"ifmodifiedsince","type":["null","string"]},{"name":"idfa","type":["null","string"]},{"name":"acceptencoding","type":["null","string"]},{"name":"acceptlanguage","type":["null","string"]},{"name":"connection","type":["null","string"]},{"name":"contentlength","type":["null","string"]},{"name":"contenttype","type":["null","string"]},{"name":"host","type":["null","string"]},{"name":"imei","type":["null","string"]},{"name":"imsi","type":["null","string"]},{"name":"mac","type":["null","string"]},{"name":"mimeagent","type":["null","string"]},{"name":"mmappid","type":["null","string"]},{"name":"mmappver","type":["null","string"]},{"name":"mmchannel","type":["null","string"]},{"name":"mmclientid","type":["null","string"]},{"name":"mmdeviceid","type":["null","string"]},{"name":"mmdownloadchannel","type":["null","string"]},{"name":"mmrid","type":["null","string"]},{"name":"mmsec","type":["null","string"]},{"name":"mmticket","type":["null","string"]},{"name":"mmticket1","type":["null","string"]},{"name":"mmticket2","type":["null","string"]},{"name":"mmticket3","type":["null","string"]},{"name":"wifiinfo","type":["null",{"type":"array","items":{"name":"wifiinfo","type":"record","fields":[{"name":"bssid","type":["null","string"]},{"name":"ssid","type":["null","string"]},{"name":"rssi","type":["null","string"]},{"name":"linkspeed","type":["null","int"]}]}}]},{"name":"mmts","type":["null","string"]},{"name":"phonebrand","type":["null","string"]},{"name":"phonetype","type":["null","string"]},{"name":"screenheight","type":["null","string"]},{"name":"screenwidth","type":["null","string"]},{"name":"tdid","type":["null","string"]},{"name":"useragent","type":["null","string"]},{"name":"versionno","type":["null","string"]},{"name":"xforwardedfor","type":["null","string"]},{"name":"xrealip","type":["null","string"]}]}},{"name":"requestparameter","type":["null","string"]},{"name":"requestbody","type":["null","string"]},{"name":"requestattribute","type":["null","string"]},{"name":"responsevalue","type":["null","string"]},{"name":"exceptionmsg","type":["null","string"]}]}"""
    val kafkaProp = Map(
      //      s"${KAFKA_PREFIX}${ConsumerConfig.GROUP_ID_CONFIG}" -> "test-group-01",
      s"${KAFKA_PREFIX}${ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG}" -> "10.27.239.167:9092,10.27.239.62:9092,10.27.239.119:9092",
      //      s"${KAFKA_PREFIX}avro.schema" -> schema,
      //      s"${KAFKA_PREFIX}${ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG}" -> "com.mime.bdp.source.KafkaAvroDeserializer",
      //      s"${KAFKA_PREFIX}${ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG}" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "subscribe" -> "etl_httplog_base"
    )

    val streamingDf = decodeAvroMessage(
      createStream(kafkaProp)
      , schema
    ).select($"value.*")

    val query = streamingDf.writeStream
      //      .outputMode("complete")
      .format("console")
      .option("truncate", "false")
      .start()

    query.awaitTermination()

  }

}
