package com.mime.bdp.fsm

/**
  *  
 *  @author yao.huang
  *  @version 1.0  
  *  @date 2018/7/11 下午4:52  
  *  @project fsm_demo
  *  */
case class TransactionImpl[T <: Event](val sourceState: State,
                                  val targetState: State,
                                  val eventType: String,
                                  val eventHandler: T => Unit) extends Transaction[T] {

  override def getEventHandler: T => Unit = eventHandler

  override def getEventType: String = eventType

  override def getSourceState: State = sourceState

  override def getTargetState: State = targetState

}
