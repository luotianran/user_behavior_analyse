package com.mime.bdp.config

/**
  *  
 *  @author yao.huang
  *  @version 1.0  
  *  @date 2018/7/9 上午10:45  
  *  @project bdp-user-analysis
  *  */
case class BatchAnalysisArguments(
                                   input: String = null,
                                   output: String = null,
                                   config: String = null,
                                   var groupBy : String = null,
                                   var eventName: String = null,
                                   var eventTime: String = null,
                                   var events: String = null,
                                   var resultTables: String = null
                                 )
