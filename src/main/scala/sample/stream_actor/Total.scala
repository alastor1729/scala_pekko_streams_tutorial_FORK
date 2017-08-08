package sample.stream_actor

import akka.Done
import akka.actor.Actor
import sample.stream_actor.Total.Increment
import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}

object Total {
  case class Increment(value: Long, avg: Double, id: String)
}

class Total extends Actor {
  var total: Long = 0

  override def receive: Receive = {
    case Increment(value, avg, id) =>
      println(s"Recieved $value new measurements from id: $id -  Avg wind speed is: $avg")
      total = total + value

      val date = new Date()
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      df.setTimeZone(TimeZone.getTimeZone("Europe/Zurich"))

      println(s"${df.format(date) } - Current total of all measurements: $total")
      sender ! Done
  }
}