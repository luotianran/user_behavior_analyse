package com.mime.bdp.fsm

import com.mime.bdp.Logging

/**
  *  
 *  @author yao.huang
  *  @version 1.0  
  *  @date 2018/7/11 下午5:13  
  *  @project fsm_demo
  *  */
class FSMImpl(val states: Seq[State], val transactions: Seq[Transaction[_]]) extends Logging {

  var currentState: State = InitailState

  def emitEvent(e: Event): Unit = {
    val allEventTypeTransactions = transactions.filter(t => states.contains(t.getTargetState)
      && states.contains(t.getSourceState)
      && t.getEventType == e.getEventType
    )

    val currentTransactions = allEventTypeTransactions.filter(t => currentState == t.getSourceState)

    if (currentTransactions.isEmpty) {
      logger.warn("链路中断...")
      // 如果是初始事件, 则重置状态
//      if (allEventTypeTransactions.head.getSourceState == InitailState) {
//        currentState = InitailState
//        emitEvent(e)
//      }
    } else {
      currentTransactions.head match {
        case t: Transaction[Event] =>

          val handler = t.getEventHandler
          if (handler != null) handler(e)

          currentState = t.getTargetState
      }
    }
  }

}

