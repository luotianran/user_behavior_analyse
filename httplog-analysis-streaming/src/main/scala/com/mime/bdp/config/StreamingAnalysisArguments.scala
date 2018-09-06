package com.mime.bdp.config

/**
  *  
 *  @author yao.huang
  *  @version 1.0  
  *  @date 2018/7/12 下午5:25  
  *  @project bdp-user-analysis
  *  */
case class StreamingAnalysisArguments(
                                       val log: String = "INFO",
                                       val config: String = null,
                                       var broker: String = null,
                                       var topics: String = null,
                                       var schema: String = null,
                                       var eventName: String = null,
                                       var eventTime: String = null,
                                       var tables : String = null,
                                       var groupBy: String = null
                                     )
