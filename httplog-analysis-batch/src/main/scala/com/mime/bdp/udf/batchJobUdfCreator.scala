package com.mime.bdp.udf

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object batchJobUdfCreator extends AbstractUdfCreator {

  def createEvent2StateUdf(event2StateMap: Map[String, String]): UserDefinedFunction = {
    udf((event: String) => event2StateMap(event))
  }

}
