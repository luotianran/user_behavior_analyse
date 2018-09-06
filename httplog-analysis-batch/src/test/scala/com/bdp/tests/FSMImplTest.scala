package com.bdp.tests

import java.util.Date

import com.mime.bdp.fsm._
import org.scalatest.FunSuite

/**
  *  
 *  @author yao.huang
  *  @version 1.0  
  *  @date 2018/7/17 下午4:28  
  *  @project bdp-user-analysis
  *  */
class FSMImplTest extends FunSuite {

  test("") {

    val eventList = "/walletWechat/api/apply/merchant/queryOrderInfo,/walletWechat/api/apply/merchant/findMerchantInfo,/walletWechat/api/apply/merchant/preTermAmt,/walletWechat/api/apply/merchant/queryMerchantTerms,/walletWechat/api/apply/merchant/merchantOrderSubmit,/walletWechat/api/apply/merchant/queryIdCardInfo,/walletWechat/api/apply/merchant/ocrResult,/walletWechat/api//apply/merchant/getIdCardBackOcrResult,/walletWechat/api/wechat/uploadImage,/walletWechat/api/apply/merchant/queryImageStatus,/walletWechat/api/apply/merchant/idCardAuthorize,/walletWechat/api/apply/merchant/queryExtBaseInfo,/walletWechat/api/common/dictSearch,/walletWechat/api/common/submitAntiFraud,/walletWechat/api/common/getPublicKey,/walletWechat/api/apply/phoneBill/submitAuthInfo,/walletWechat/api/apply/phoneBill/submitVerifyCode,/walletWechat/api/apply/phoneBill/resendVerifyCode,/walletWechat/api/apply/phoneBill/skip,/walletWechat/api/apply/merchant/mobileAuthorize,/walletWechat/api/apply/merchant/submitApplyVerifyCode,/walletWechat/api/apply/merchant/submitNonStudentApplyInfo".split(",")
    val uriStateMap = eventList.zipWithIndex.map {
      case (uri: String, i: Int) => uri -> s"e${i}"
    }.toMap

    val transactions = (for (i <- 0 until eventList.size) yield {
      val eventType = eventList(i)
      val (sourceState, targetState) = if (i == 0) InitailState -> State(uriStateMap(eventType)) else State(uriStateMap(eventList(i - 1))) -> State(uriStateMap(eventType))
      new TransactionImpl[DefaultEvent](sourceState, targetState, eventType, null)
    }) ++ Seq(
      new TransactionImpl[DefaultEvent](State("e1"), State("e3"), "/walletWechat/api/apply/merchant/queryMerchantTerms", null),
    new TransactionImpl[DefaultEvent](State("e3"), State("e0"), "/walletWechat/api/apply/merchant/queryOrderInfo", null)
    )

    val states = transactions.flatMap(t => Seq(t.getSourceState, t.getTargetState)).toSet.toSeq

    val fsm = new FSMImpl(states, transactions)
    val e0 = new DefaultEvent("/walletWechat/api/apply/merchant/queryOrderInfo", new Date().getTime)
    fsm.emitEvent(e0)
    println(fsm.currentState.name)
    val e1 = new DefaultEvent("/walletWechat/api/apply/merchant/findMerchantInfo", new Date().getTime)
    fsm.emitEvent(e1)
    println(fsm.currentState.name)
    val e3 = new DefaultEvent("/walletWechat/api/apply/merchant/queryMerchantTerms", new Date().getTime)
    fsm.emitEvent(e3)
    fsm.emitEvent(e0)
    println(fsm.currentState.name)
  }
}
