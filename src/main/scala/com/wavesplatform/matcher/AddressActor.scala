package com.wavesplatform.matcher

import akka.actor.{Actor, ActorRef}
import com.wavesplatform.account.Address
import com.wavesplatform.matcher.api.{OrderCancelRejected, OrderRejected}
import com.wavesplatform.matcher.model.{Events, LimitOrder, OrderValidator}
import com.wavesplatform.state.{ByteStr, Portfolio}
import com.wavesplatform.transaction.AssetId
import com.wavesplatform.transaction.assets.exchange.{AssetPair, Order}

import scala.collection.mutable

class AddressActor(sender: Address, portfolio: => Portfolio, matcherRef: ActorRef) extends Actor {
  import AddressActor._

  private val activeOrders  = mutable.AnyRefMap.empty[ByteStr, LimitOrder]
  private val openVolume    = mutable.AnyRefMap.empty[Option[AssetId], Long].withDefaultValue(0L)
  private var latestOrderTs = 0L

  private def reserve(limitOrder: LimitOrder): Unit = {
    openVolume += limitOrder.spentAsset -> (openVolume(limitOrder.spentAsset) + limitOrder.getSpendAmount)
    openVolume += limitOrder.feeAsset   -> (openVolume(limitOrder.feeAsset) + limitOrder.fee)
  }

  private def release(limitOrder: LimitOrder): Unit = {
    openVolume += limitOrder.spentAsset -> (openVolume(limitOrder.spentAsset) - limitOrder.getSpendAmount)
    openVolume += limitOrder.feeAsset   -> (openVolume(limitOrder.feeAsset) - limitOrder.fee)
  }

  private def tradableBalance(assetId: Option[AssetId]): Long =
    assetId.fold(portfolio.spendableBalance) { id =>
      portfolio.assets.getOrElse(id, 0)
    } - openVolume(assetId)

  private val validator = OrderValidator.accountStateAware(sender, tradableBalance, activeOrders.size, latestOrderTs) _

  def receive: Receive = {
    case o: Order =>
      validator(o) match {
        case Right(_) =>
          val lo = LimitOrder(o)
          activeOrders += o.id() -> lo
          reserve(lo)
          latestOrderTs = latestOrderTs.max(lo.order.timestamp)
          matcherRef.forward(o)
        case Left(error) =>
          sender() ! OrderRejected(error)
      }
    case CancelOrder(id) =>
      activeOrders.get(id) match {
        case Some(lo) =>
          activeOrders -= id
          release(lo)
          matcherRef.forward(Events.OrderCanceled(lo, false))
        case None => sender() ! OrderCancelRejected(s"Order $id not found")
      }
    case CancelAllOrders(maybePair, timestamp) =>
      sender() ! OrderCancelRejected(s"Not implemented")
    case GetOrderStatus(orderId) =>
      activeOrders.get(orderId) match {
        case Some(lo) =>
        case None     =>
      }
    case GetTradableBalance(pair) =>
    case GetReservedBalance       =>
  }
}

object AddressActor {
  sealed trait Command

  case class GetOrderStatus(orderId: ByteStr)                          extends Command
  case class GetActiveOrders(assetPair: Option[AssetPair])             extends Command
  case class GetTradableBalance(assetPair: AssetPair)                  extends Command
  case object GetReservedBalance                                       extends Command
  case class PlaceOrder(order: Order)                                  extends Command
  case class CancelOrder(orderId: ByteStr)                             extends Command
  case class CancelAllOrders(pair: Option[AssetPair], timestamp: Long) extends Command
}
