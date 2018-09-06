package com.mime.bdp.config

import java.io.{File, FileInputStream}
import java.util.Properties
import com.mime.bdp.config.Constants._

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

  private[config] val parser = new OptionParser[BatchAnalysisArguments]("httplog-analysis-batch") {

    opt[String]("input").required().action((x, c) => {
      c.copy(input = x)
    })

    opt[String]("output").required().action((x, c) => {
      c.copy(output = x)
    })

    opt[String]("config").action((x, c) => {
      c.copy(config = x)
    })

    opt[String](KEY_TABLES).action((x, c) => {
      c.copy(resultTables = x)
    })

    opt[String](KEY_EVENT_TIME).action((x, c) => {
      c.copy(eventTime = x)
    })

    opt[String](KEY_EVENT_NAME).action((x, c) => {
      c.copy(eventName = x)
    })

    opt[String](KEY_EVENT_LIST).action((x, c) => {
      c.copy(events = x)
    })

    opt[String](KEY_GROUP_BY).action((x, c) => {
      c.copy(groupBy = x)
    })
  }

  def parse(args: Array[String]): BatchAnalysisArguments = {
    val cmdArg = parser.parse(args, BatchAnalysisArguments()).get

    val conf = Try(cmdArg.config match {
      case  filename: String =>
        val prop = new Properties()
        val in = new FileInputStream(new File(filename))
        prop.load(in)
        in.close()
        prop
      case _=> new Properties()
    }).getOrElse(new Properties())

    cmdArg.eventTime = Option(cmdArg.eventTime).getOrElse(readConf(conf, KEY_EVENT_TIME))
    cmdArg.eventName = Option(cmdArg.eventName).getOrElse(readConf(conf, KEY_EVENT_NAME))
    cmdArg.events = Option(cmdArg.events).getOrElse(readConf(conf, KEY_EVENT_LIST))
    cmdArg.resultTables= Option(cmdArg.resultTables).getOrElse(readConf(conf, KEY_TABLES))
    cmdArg.groupBy = Option(cmdArg.groupBy).getOrElse(readConf(conf, KEY_GROUP_BY))


    require(cmdArg.eventName != null, s"field '${KEY_EVENT_NAME}' should not be null")
    require(cmdArg.eventTime != null, s"field '${KEY_EVENT_TIME}' should not be null")
    require(cmdArg.events != null, s"field '${KEY_EVENT_LIST}' should not be null")
    require(cmdArg.resultTables != null, s"field '${KEY_TABLES}' should not be null")
//    require(cmdArg.start != null, s"field '${KEY_START}' should not be null")
//    require(cmdArg.end != null, s"field '${KEY_END}' should not be null")
    require(cmdArg.groupBy != null, s"field '${KEY_GROUP_BY}' should not be null")

    cmdArg
  }
  private[config] def readConf(conf: Properties, key: String): String = {
    if (conf.containsKey(key)) conf.get(key).toString else null
  }


}
