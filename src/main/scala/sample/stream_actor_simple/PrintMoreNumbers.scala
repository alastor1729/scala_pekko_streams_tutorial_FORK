package sample.stream_actor_simple

import org.apache.pekko.actor.{Actor, ActorSystem}
import org.apache.pekko.stream.scaladsl.{Keep, Sink, Source}
import org.apache.pekko.stream.{KillSwitches, UniqueKillSwitch}

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.*

class PrintMoreNumbers extends Actor {
  implicit val system: ActorSystem = context.system
  implicit val executionContext: ExecutionContextExecutor = context.system.dispatcher

  private val (killSwitch: UniqueKillSwitch, done) =
    Source.tick(0.seconds, 1.second, 1)
      .scan(0)(_ + _)
      .map(_.toString)
      .viaMat(KillSwitches.single)(Keep.right)
      .toMat(Sink.foreach(println))(Keep.both)
      .run()

  done.map(_ => self ! "done")

  override def receive: Receive = {
    //When the actor is stopped, it will also stop the stream
    case "stop" =>
      println("Stopping...")
      killSwitch.shutdown()
    case "done" =>
      println("Done")
      context.stop(self)
      context.system.terminate()
  }
}
