import akka.actor.Actor
import akka.actor.Actor._
import akka.actor.Props
import akka.actor.ActorSystem
import scala.util.Random
import scala.concurrent.duration.Duration
import scala.collection.mutable.ArrayBuffer
import akka.dispatch.ExecutionContexts
import scala.concurrent.ExecutionContext
import scala.compat.Platform
import sun.reflect.MagicAccessorImpl
import akka.actor.Kill
import akka.actor.ActorRef

object Project2 {
  def main(args: Array[String]) {
    if (args.length != 3) {
      println("INVALID INPUT!! EXITTING..");
      System.exit(0)
    } else {
      val messageSep = ","
      val conxt = ActorSystem("project2")
      val controller = conxt.actorOf(Props(new Controller(args(0).toInt, args(1), args(2), messageSep)), "controller");
    }
  }
}

class Controller(numNodes: Int, topology: String, algo: String, sep: String) extends Actor {

  var time: Long = 0
  val n = numNodes
  var workerStatus = Array.fill[Boolean](n)(false)
  val workerContext = ActorSystem("project2")
  for (i <- 1 to n) {
    var msg: String = "initialiseWorker"
    val node = workerContext.actorOf(Props(new Worker), "Node" + i.toString)
    node ! msg
  }
  println("Algorithm Selected => " + algo)
  println("Topology => " + topology)
  var top = topology.toLowerCase()
  top match {
    case "line" =>
      val al = "l"
      println("Creating " + n + " line network worker nodes")
      for (i <- 1 to n) {
        var msg: String = al + i.toString + sep
        val worker = workerContext.actorSelection("/user/Node" + i)
        val neighbr1 = i - 1
        val neihbr2 = i + 1
        if (i > 1)
          msg = msg + neighbr1.toString + sep
        if (i < n)
          msg = msg + neihbr2.toString
        println("Worker " + i + " gets neighbor info -> " + msg)
        worker ! msg
      }
    case "full" =>
      println("Creating " + n + " full network worker nodes")
      val al = "f"
      for (i <- 1 to n) {
        var msg: String = al + n.toString
        val worker = workerContext.actorSelection("/user/Node" + i)
        msg = msg + sep + i.toString
        println("Worker " + i + " gets neighbor info -> " + msg)
        worker ! msg
      }
    case "2d" =>
      val nearesrRoot = math.sqrt(n).floor.toInt
      val NextSquareNumber = math.pow(nearesrRoot, 2).toInt
      println("Creating " + NextSquareNumber + " worker nodes for perfect 2D")
      println("Getting neighbors in 2D..")
      val al = "2"
      for (i <- 1 to NextSquareNumber) {
        var msg: String = al + i.toString
        val worker = workerContext.actorSelection("/user/Node" + i)
        
        if (i - nearesrRoot > 0) 
          msg = msg + sep + (i - nearesrRoot).toString 
        if (i + nearesrRoot <= NextSquareNumber) 
          msg = msg + sep + (i + nearesrRoot).toString
        if (i % nearesrRoot == 0) 
          msg = msg + sep + (i - 1).toString
        else if (i % nearesrRoot == 1) 
          msg = msg + sep + (i + 1).toString
        else 
          msg = msg + sep + (i - 1).toString + sep + (i + 1).toString
        println("Worker " + i + " gets neighbor info -> " + msg)
        worker ! msg
      }
    case "imp2d" =>
      val nearestiRoot = math.sqrt(n).floor.toInt
      val nextSq = math.pow(nearestiRoot, 2).toInt
      var rndInt = Int.MaxValue
      val al = "m"
      while (rndInt > nextSq || rndInt == 0) {
        rndInt = (Random.nextInt % nextSq).abs
      }
      for (i <- 1 to nextSq) {
        var msg: String = al + i.toString
        val worker = workerContext.actorSelection("/user/Node" + i)
        if (i - nearestiRoot > 0) 
          msg = msg + sep + (i - nearestiRoot).toString
        if (i + nearestiRoot <= nextSq) 
          msg = msg + sep + (i + nearestiRoot).toString
        if (i % nearestiRoot == 0) 
          msg = msg + sep + (i - 1).toString
        else if (i % nearestiRoot == 1) 
          msg = msg + sep + (i + 1).toString
        else 
          msg = msg + sep + (i - 1).toString + sep + (i + 1).toString
        msg = msg + sep + rndInt.toString
        println("Worker " + i + " gets neighbor info -> " + msg)
        worker ! msg
      }
    case others=>
      println("Unrecognized algo specified")
      System.exit(0)
  }
  println("Topology Built")
  var alg = algo.toLowerCase()
  var gossip = "g"
  alg match {
    case "gossip" =>
      
      val msg: String = gossip + "THIS IS A RUMOR"
      var rndInt = Int.MaxValue
      while (rndInt > n || rndInt == 0) {
        rndInt = (Random.nextInt % n).abs
      }
      val worker = workerContext.actorSelection("/user/Node" + rndInt)
      println("Worker " + rndInt + " -> " + msg)
      worker ! msg
    case "push-sum" =>
      val msg: String = "p" + 0.0.toString + sep + 0.0.toString
      var rndInt = Int.MaxValue
      while (rndInt > n || rndInt == 0) {
        rndInt = (Random.nextInt % n).abs
      }
      val worker = workerContext.actorSelection("/user/Node" + rndInt)
      println("Worker " + rndInt + " -> " + msg)
      worker ! msg
    case whatever =>
      println("error")
  }
  println("Starting time measurement")
  time = System.currentTimeMillis()
  def receive = {
    case l: String =>
      l.toLowerCase().head match {
        case 'd' => 
          
          var revMsg = l.tail.split(sep)
          println("Controller gets message from worker ID " + revMsg(0).toInt)
          //end condition
          
          workerStatus.update((revMsg(0).toInt - 1), true)
          var workerCount = 0;
          for (x <- workerStatus)
            if (x == true) workerCount += 1
          var temptime = System.currentTimeMillis() - time
          println("=========================================================>Time taken: " + (temptime).toString + "ms")
          if (revMsg.size <= 1) {
            var percentCovered = (workerCount * 100.0 / n)
            println("Percentage Actors Covered: " + percentCovered.toString)
            if (percentCovered >= 90.0) {
              context.children.foreach(context.stop(_))
              context.stop(self)
              println("FINAL TIME ===============================================> " + temptime  + "ms")
              System.exit(0); 
              
            } 
          } else {
            println("End Ratio: " + revMsg(1))
            context.children.foreach(context.stop(_))
            context.stop(self)
            System.exit(0);
          }
        case 'e' => //error
          println("RECIEVED TERMINATE SIGNAL FROM WORKER " + l.tail)
          var temptime = System.currentTimeMillis() - time
          context.children.foreach(context.stop(_))
          context.stop(self)
          println("FINAL TIME =====> " + temptime  + "ms")
          System.exit(0);
          context.children.foreach(context.stop(_))
          println("stopped children, now I quit!")
          context.stop(context.self)
        case whatever =>
          println("Controller got this " + whatever)
      }
  }
}
class Worker extends Actor {
  import context._
  var limit = 100
  var sep = ","
  var controller: ActorRef = null
  var myMsgRecvCount = 0
  var isGossip = true
  var rumor = ""
  var s = 0.0; 
  var w = 1.0; 
  var ratio = 0.0; 
  var ratio_old = 999.0; 
  var dratio = 999.0;
  var staticCount = 0
  var ratioCount = 0
  var neighbours = ArrayBuffer[String]()
  def transmitMsg = {
    val resendGossipTrue = "rt"
    val resendGossipeFalse = "rf"
    val msg = 
      	if (isGossip == true) 
      		resendGossipTrue + rumor 
      	else 
      		resendGossipeFalse + sep + s.toString + sep + w.toString
    val len = neighbours.length
    println("current neighbours for worker " + neighbours(0) + " are " + neighbours .mkString(" "))
    var rndInt = Int.MaxValue
    while (rndInt > len || rndInt == 0) {
      
      rndInt = (Random.nextInt % len).abs
      println("Randomly selected neighbour = worker " + rndInt)
    }
    val selectNeighbour = context.actorSelection("/user/Node" + neighbours(rndInt))
    selectNeighbour ! msg
    println("Worker "+neighbours(0)+" to worker " +neighbours(rndInt) +" -> " + msg)
  }
  def receive = {
    case l: String =>
      l.head match {
        
         //initialize
        case 'i' =>
          controller = sender
        
          //line
        case 'l' =>
          neighbours ++= l.tail.split(sep)
        case 'r' =>
          l.tail.head match {
            case 't' => isGossip = true
            case 'f' => isGossip = false
          }
          if (isGossip == true && myMsgRecvCount < limit) {
            println("Worker "+neighbours(0) + " incrementing recieve message count by one to " + (myMsgRecvCount+1).toString)
            myMsgRecvCount += 1
            rumor = l.tail.tail
            val sendmsg = "d" + neighbours(0);
            println("Worker " + neighbours(0) + " to CONTROLLER -> " + sendmsg)
            controller ! sendmsg;
            transmitMsg
          }
          //need re-transmission
          if (isGossip == true && staticCount < 5 && myMsgRecvCount < limit - 1) {
            val dur = Duration.create(50, scala.concurrent.duration.MILLISECONDS);
            val me = context.self
            context.system.scheduler.scheduleOnce(dur, me, "z") 
            
          } else if (myMsgRecvCount == limit) {
            myMsgRecvCount += 1;
            val sendmsg = "d" + neighbours(0);
            println("Worker "+ neighbours(0) +" to CONTROLLER -> " + sendmsg)
            controller ! sendmsg;
            //end
    //        controller ! "e" + neighbours(0)
            println(neighbours(0) + " has got ten messages. ")
          } else if (isGossip == false) {
            if (s == 0.0) {
              s = neighbours(0).toDouble
            }
            var inParams = l.tail.tail.split(sep)
            s = (s + inParams(1).toDouble) / 2
            w = (w + inParams(2).toDouble) / 2
            ratio_old = ratio
            ratio = s / w
            println(self + ratio.toString)
            dratio = (ratio - ratio_old).abs
            if (dratio < 0.0000000001) ratioCount += 1
            if (ratioCount == 3) {
              val sendmsg = "d" + neighbours(0) + sep + ratio.toString;
              println("Worker " + neighbours(0) + " to CONTROLLER -> " + sendmsg)
              controller ! sendmsg;
            } else if (ratioCount < 3) {
              transmitMsg
            } else if (ratioCount > 3) {
              println("Ratio already reached!!")
            }
          }
          
          //full
        case 'f' =>
          println("Case full starting worker")
          val input = l.tail.split(sep)
          val numNodes = input(0).toInt
          val myNumber = input(1)
          for (i <- 1 to numNodes)
            neighbours += i.toString
          neighbours.update(0, myNumber)
          neighbours.update((myNumber.toInt - 1), 1.toString)
        case '2' =>
          neighbours ++= l.tail.split(sep)
        case 'm' =>
          neighbours ++= l.tail.split(sep)
        case 'o' =>
          neighbours ++= l.tail.split(sep)
        case 'z' =>
          transmitMsg
        case 'g' =>
          println("setting gossip mode = true in worker " + neighbours(0))
          isGossip = true
          rumor = l.tail
          val sendmsg = "d" + neighbours(0);
          controller ! sendmsg;
          println("Worker " + neighbours(0) + " to CONTROLLER -> " + sendmsg)
          transmitMsg
        case 'p' =>
          isGossip = false
          s = neighbours(0).toDouble
          ratio = s / w
          dratio = (ratio_old - ratio).abs
          transmitMsg
        case others =>
          println("error!")
      }
  }
}