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
      // log.debug("Starting BinaryTreeSet.Insert id=" + id + " elem=" + elem)
      root ! Insert(requester, id, elem)
    }
    case Contains(requester, id, elem) => {
      // log.debug("Starting BinaryTreeSet.Contains id=" + id + " elem=" + elem) 
      root ! Contains(requester, id, elem)
    }
    case Remove(requester, id, elem) => {
      // log.debug("Starting BinaryTreeSet.Remove id=" + id + " elem=" + elem) 
      root ! Remove(requester, id, elem)
    }
    case GC => {
      val newRoot = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))
      context.become(garbageCollecting(newRoot))
      root ! CopyTo(newRoot)
    }
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = LoggingReceive {
    case Insert(requester, id, elem) => {
      // log.debug("Add Insert to queue id=" + id + " elem=" + elem)
      pendingQueue = pendingQueue enqueue Insert(requester, id, elem)
    }
    case Contains(requester, id, elem) => {
      // log.debug("Add Contains to queue id=" + id + " elem=" + elem)
      pendingQueue = pendingQueue enqueue Contains(requester, id, elem)
    }
    case Remove(requester, id, elem) => {
      // log.debug("Add Remove to queue id=" + id + " elem=" + elem)
      pendingQueue = pendingQueue enqueue Remove(requester, id, elem)
    }
    case CopyFinished => {
      // log.debug("CopyFinished")
      root = newRoot
      while (!pendingQueue.isEmpty) {
        val (ops, newQueue) = pendingQueue.dequeue
        newRoot ! ops
        pendingQueue = newQueue
      }
      context.become(normal)
    }
  }
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
      // log.debug("Starting BinaryTreeNode.Insert id=" + id + " em=" + em)
      if (em == elem) {
        if (removed) {
          removed = false
        }
        requester ! OperationFinished(id)
        // log.debug("Already inserted node id=" + id + " em=" + em)
      } else if (em < elem) {
        if (subtrees contains Left) {
          val leftTree = subtrees(Left)
          leftTree ! Insert(requester, id, em)
        } else {
          val leftNode = context.actorOf(BinaryTreeNode.props(em, false))
          subtrees += (Left -> leftNode)
          requester ! OperationFinished(id)
          // log.debug("Inserted left node id=" + id + " em=" + em)
        }
      } else {
        if (subtrees contains Right) {
          val rightTree = subtrees(Right)
          rightTree ! Insert(requester, id, em)
        } else {
          val rightNode = context.actorOf(BinaryTreeNode.props(em, false))
          subtrees += (Right -> rightNode)
          requester ! OperationFinished(id)
          // log.debug("Inserted right node=" + id + " em=" + em)
        }
      }
    }

    case Contains(requester, id, em) => {
      // log.debug("Starting BinaryTreeNode.Contains id=" + id + " em=" + em)
      if (em == elem) {
        if (removed) {
          requester ! ContainsResult(id, false)
          // log.debug("Node not found on id=" + id + " em=" + em)
        } else {
          requester ! ContainsResult(id, true)
          // log.debug("Found node id=" + id + " em=" + em)
        }
      } else if (em < elem) {
        if (subtrees contains Left) {
          val leftTree = subtrees(Left)
          leftTree ! Contains(requester, id, em)
        } else {
          requester ! ContainsResult(id, false)
          // log.debug("Node not found on left id=" + id + " em=" + em)
        }
      } else {
        if (subtrees contains Right) {
          val rightTree = subtrees(Right)
          rightTree ! Contains(requester, id, em)
        } else {
          requester ! ContainsResult(id, false)
          // log.debug("Node not found on right id=" + id + " em=" + em)
        }
      }
    }

    case Remove(requester, id, em) => {
      // log.debug("Starting BinaryTreeNode.Remove id=" + id + " em=" + em)
      if (em == elem) {
        if (!removed) {
          removed = true
        }
        requester ! OperationFinished(id)
        // log.debug("Removed node id=" + id + " em=" + em)
      } else if (em < elem) {
        if (subtrees contains Left) {
          val leftTree = subtrees(Left)
          leftTree ! Remove(requester, id, em)
        } else {
          requester ! OperationFinished(id)
          // log.debug("Missing left node id=" + id + " em=" + em)
        }
      } else {
        if (subtrees contains Right) {
          val rightTree = subtrees(Right)
          rightTree ! Remove(requester, id, em)
        } else {
          requester ! OperationFinished(id)
          // log.debug("Missing right node id=" + id + " em=" + em)
        }
      }
    }

    case CopyTo(treeNode) => {
      if (removed && subtrees.isEmpty) {
        context.parent ! CopyFinished
        // log.debug("Do not copy removed leaf node elem=" + elem)
      } else {
        var expected = Set[ActorRef]()
        if (subtrees contains Left) {
          expected += subtrees(Left)
        }
        if (subtrees contains Right) {
          expected += subtrees(Right)
        }
      
        if (removed) {
          context.become(copying(expected, true))
          // log.debug("Copy subtrees of removed node elem=" + elem)
        } else {
          context.become(copying(expected, false))
          treeNode ! Insert(self, 0, elem)
          // log.debug("Copy node with subtrees elem=" + elem)
        }
        subtrees.values foreach {_ ! CopyTo(treeNode)}
      }
    }
  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = LoggingReceive {
    case OperationFinished(_) => {
      if (expected.isEmpty) {
        context.parent ! CopyFinished
        context.become(normal)
        // log.debug("CopyFinished after insert")
      } else {
        context.become(copying(expected, true))
      }
    }
    case CopyFinished => {
      val waiting = expected - sender
      if (waiting.isEmpty && insertConfirmed) {
        context.parent ! CopyFinished
        context.become(normal)
        // log.debug("CopyFinished after copying subtrees")
      } else {
        context.become(copying(waiting, insertConfirmed))
      }
    }
  }
}
