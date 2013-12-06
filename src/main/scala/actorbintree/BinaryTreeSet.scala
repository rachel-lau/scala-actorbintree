/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor._
import akka.event._
import scala.collection.immutable.Queue

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef
    def id: Int
    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection*/
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply
  
  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}


class BinaryTreeSet extends Actor {
  import BinaryTreeSet._
  import BinaryTreeNode._

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  val log = Logging(context.system, this)

  // optional
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = LoggingReceive { 
    case Insert(requester, id, elem) => {
      log.debug("Starting BinaryTreeSet.Insert id=" + id + " elem=" + elem)
      root ! Insert(requester, id, elem)
      context.become(awaitInsert(requester))
    }
    case Contains(requester, id, elem) => {
      log.debug("Starting BinaryTreeSet.Contains id=" + id + " elem=" + elem) 
      root ! Contains(requester, id, elem)
      context.become(awaitContains(requester))
    }
  }

  def awaitInsert(requester: ActorRef) : Receive = LoggingReceive {
    case OperationFinished(id) => {
      log.debug("Starting BinaryTreeSet.awaitInsert")
      requester ! OperationFinished(id)
      context.unbecome()
    }
  }

  def awaitContains(requester: ActorRef) : Receive = LoggingReceive {
    case ContainsResult(id, result) => {
      log.debug("Starting BinaryTreeSet.awaitContains")
      requester ! ContainsResult(id, result)
      context.unbecome()
    }
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = ???

}

object BinaryTreeNode {
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode],  elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor {
  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  val log = Logging(context.system, this)

  // optional
  def receive = normal

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = LoggingReceive { 
    case Insert(requester, id, em) => {
      log.debug("Starting BinaryTreeNode.Insert id=" + id + " em=" + em)
      if (em == elem) {
        sender ! OperationFinished(id)
        log.debug("Already inserted node")
      } else if (em < elem) {
        if (subtrees contains Left) {
          val leftTree = subtrees(Left)
        } else {
          val leftNode = context.actorOf(BinaryTreeNode.props(em, false))
          subtrees += (Left -> leftNode)
          sender ! OperationFinished(id)
          log.debug("Inserted left node")
        }
      } else {
        if (subtrees contains Right) {
          val rightTree = subtrees(Right)
        } else {
          val rightNode = context.actorOf(BinaryTreeNode.props(em, false))
          subtrees += (Right -> rightNode)
          sender ! OperationFinished(id)
          log.debug("Inserted right node")
        }
      }
    }

    case Contains(requester, id, em) => {
      log.debug("Starting BinaryTreeNode.Contains id=" + id + " em=" + em)
      if (em == elem) {
        sender ! ContainsResult(id, true)
        log.debug("Found node")
      } else if (em < elem) {
        if (subtrees contains Left) {
          val leftTree = subtrees(Left)
          leftTree ! Contains(requester, id, em)
          context.become(awaitContains())
        } else {
          sender ! ContainsResult(id, false)
          log.debug("Node not found on left")
        }
      } else {
        if (subtrees contains Right) {
          val rightTree = subtrees(Right)
          rightTree ! Contains(requester, id, em)
          context.become(awaitContains())
        } else {
          sender ! ContainsResult(id, false)
          log.debug("Node not found on right")
        }
      }
    }
  }

  def awaitContains() : Receive = LoggingReceive {
    case ContainsResult(id, result) => {
      log.debug("Starting BinaryTreeNode.awaitContains")
      sender ! ContainsResult(id, result)
      context.unbecome()
    }
  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = ???

}
