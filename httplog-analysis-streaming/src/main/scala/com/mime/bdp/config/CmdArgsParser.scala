package com.mime.bdp.config

import java.io.{File, FileInputStream}
import java.util.Properties

import scopt.OptionParser

import scala.util.Try

/**
  *  
 *  @author yao.huang
  *  @version 1.0  
  *  @date 2018/7/9 上午10:43  
  *  @project bdp-user-analysis
  *  */
object CmdArgsParser {

  import Constants._

  private[config] val parser = new OptionParser[StreamingAnalysisArguments]("httplog-analysis-batch") {

    opt[String]("config").action((x, c) => {
      c.copy(config = x)
    })

    opt[String](KEY_KAFKA_BROKER).action((x, c) => {
      c.copy(broker = x)
    })

    opt[String](KEY_KAFKA_TOPICS).action((x, c) => {
      c.copy(topics = x)
    })

    opt[String](KEY_KAFKA_SCHEMA).action((x, c) => {
      c.copy(schema = x)
    })

    opt[String](KEY_TABLES).action((x, c) => {
      c.copy(tables = x)
    })

    opt[String](KEY_EVENT_TIME).action((x, c) => {
      c.copy(eventTime = x)
    })

    opt[String](KEY_EVENT_NAME).action((x, c) => {
      c.copy(eventName = x)
    })

    opt[String](KEY_GROUP_BY).action((x, c) => {
      c.copy(groupBy = x)
    })
  }

  def parse(args: Array[String]): StreamingAnalysisArguments = {
    val cmdArg = parser.parse(args, StreamingAnalysisArguments()).get

    val conf = Try(cmdArg.config match {
      case  filename: String =>
        val prop = new Properties()
        val in = new FileInputStream(new File(filename))
        prop.load(in)
        in.close()
        prop
      case _=> new Properties()
    }).getOrElse(new Properties())

    cmdArg.broker = Option(cmdArg.broker).getOrElse(readConf(conf, KEY_KAFKA_BROKER))
    cmdArg.topics = Option(cmdArg.topics).getOrElse(readConf(conf, KEY_KAFKA_TOPICS))
    cmdArg.schema = Option(cmdArg.schema).getOrElse(readConf(conf, KEY_KAFKA_SCHEMA))

    cmdArg.eventTime = Option(cmdArg.eventTime).getOrElse(readConf(conf, KEY_EVENT_TIME))
    cmdArg.eventName = Option(cmdArg.eventName).getOrElse(readConf(conf, KEY_EVENT_NAME))
    cmdArg.tables = Option(cmdArg.tables).getOrElse(readConf(conf, KEY_TABLES))
    cmdArg.groupBy = Option(cmdArg.groupBy).getOrElse(readConf(conf, KEY_GROUP_BY))

    require(cmdArg.broker != null, s"field '${KEY_KAFKA_BROKER}' should not be null")
    require(cmdArg.topics != null, s"field '${KEY_KAFKA_TOPICS}' should not be null")

    require(cmdArg.eventName != null, s"field '${KEY_EVENT_NAME}' should not be null")
    require(cmdArg.eventTime != null, s"field '${KEY_EVENT_TIME}' should not be null")
    require(cmdArg.tables != null, s"field '${KEY_TABLES}' should not be null")
    require(cmdArg.groupBy != null, s"field '${KEY_GROUP_BY}' should not be null")

    cmdArg
  }

  private[config] def readConf(conf: Properties, key: String): String = {
    if (conf.containsKey(key)) conf.get(key).toString else null
  }


}
