package com.mime.bdp.fsm

/**
  *  
 *  @author yao.huang
  *  @version 1.0  
  *  @date 2018/7/17 下午3:36  
  *  @project bdp-user-analysis
  *  */
case class DefaultEvent(val eventType: String, val timestamp: Long) extends Event {
  override def getEventType: String = eventType
}
