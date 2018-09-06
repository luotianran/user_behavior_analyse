package com.mime.bdp.stream

import org.scalatest.{BeforeAndAfter, FunSuite}
import com.mime.bdp.config.CmdArgsParser._

class ConfigTests extends FunSuite with BeforeAndAfter {

  test("test") {
    val args = Array("--config=/Users/clover/Desktop/work_mime/bigdata/bdp-user-analysis/httplog-analysis-streaming/src/test/resources/config.properties")
    val config = parse(args)

    println(config)

  }

}
