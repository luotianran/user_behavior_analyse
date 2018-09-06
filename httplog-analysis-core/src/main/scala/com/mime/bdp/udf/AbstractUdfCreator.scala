package com.mime.bdp.udf

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

abstract class AbstractUdfCreator {

  def createEventFilterUdf(eventList: Seq[String]): UserDefinedFunction = {
    udf((event: String) => eventList.contains(event))
  }

}
