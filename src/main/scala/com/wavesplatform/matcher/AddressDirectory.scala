package com.wavesplatform.matcher

import akka.actor.{Actor, ActorRef, Props, SupervisorStrategy, Terminated}
import com.wavesplatform.account.Address
import com.wavesplatform.state.{EitherExt2, Portfolio}
import com.wavesplatform.utils.ScorexLogging

import scala.collection.mutable

class AddressDirectory(portfolio: Address => Portfolio, matcherRef: ActorRef) extends Actor with ScorexLogging {
  import AddressDirectory._
  import context._

  private[this] val children = mutable.AnyRefMap.empty[Address, ActorRef]

  override def supervisorStrategy: SupervisorStrategy = SupervisorStrategy.stoppingStrategy

  private def createAddressActor(address: Address): ActorRef =
    watch(actorOf(Props(new AddressActor(address, portfolio(address), matcherRef)), address.toString))

  override def receive: Receive = {
    case Envelope(address, cmd) =>
      children.getOrElseUpdate(address, createAddressActor(address)).forward(cmd)
    case Terminated(child) =>
      val addressString = child.path.name
      val address       = Address.fromString(addressString).explicitGet()
      children.remove(address)
      log.warn(s"Address handler for $addressString terminated")
  }
}

object AddressDirectory {
  case class Envelope(address: Address, cmd: AddressActor.Command)
}
