package com.mime.bdp.fsm

/**
  *  
 *  @author yao.huang
  *  @version 1.0  
  *  @date 2018/7/11 下午4:49  
  *  @project fsm_demo
  *  */
trait Transaction[T <: Event] {

  def getEventHandler: T => Unit

  def getEventType: String

  def getSourceState: State

  def getTargetState: State

}
