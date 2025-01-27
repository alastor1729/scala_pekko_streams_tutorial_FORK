package sample.stream_actor

import io.circe.generic.auto.*
import org.apache.pekko.Done
import org.apache.pekko.actor.{ActorSystem, Props}
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.Http.ServerBinding
import org.apache.pekko.http.scaladsl.model.ws.{Message, TextMessage}
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.stream.scaladsl.{Flow, Source}
import org.apache.pekko.util.Timeout
import org.slf4j.{Logger, LoggerFactory}
import sample.stream_actor.Total.Increment

import java.time.LocalTime
import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success}


/**
  * Sample Implementation of:
  * http://blogger.colinbreck.com/integrating-akka-streams-and-akka-actors-part-i
  *
  * WindTurbineServer receives [[Measurements]] via Websockets from n clients
  * Clients are started with [[SimulateWindTurbines]]
  *
  */
object WindTurbineServer {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system: ActorSystem = ActorSystem()

  import system.dispatcher

  object Messages {

    def parse(messages: immutable.Seq[String]): Seq[MeasurementsContainer] = messages.map { message =>
      io.circe.parser
        .parse(message)
        .flatMap(_.as[MeasurementsContainer])
        .toOption.get
    }

    def ack(aString: String): TextMessage = TextMessage(Source.single("Ack from server: " + aString))
  }


  def main(args: Array[String]): Unit = {
    val total = system.actorOf(Props[Total](), "total")

    def average[T](ts: Iterable[T])(implicit num: Numeric[T]) = {
      val avg = num.toDouble(ts.sum) / ts.size
      (math floor avg * 100) / 100
    }

    // compute intermediate sums (= max 100 measurements at least every second)
    // and send them to the Total actor
    val measurementsWebSocketFlow: Flow[Message, Message, Any] =
    Flow[Message]
      .collect {
        case TextMessage.Strict(text) =>
          Future.successful(text)
        case TextMessage.Streamed(textStream) =>
          textStream.runFold("")(_ + _)
            .flatMap(Future.successful)
      }
      .mapAsync(1)(identity)
      .groupedWithin(100, 1.second)
      .map(messages => (messages.last, Messages.parse(messages)))
      //.wireTap(elem => println(s"After parsing size: ${elem._2.size}"))
      .mapAsync(1) {
        case (lastMessage: String, measurements: Seq[MeasurementsContainer]) =>
          import org.apache.pekko.pattern.ask
          implicit val askTimeout: Timeout = Timeout(30.seconds)

          //generateRandomServerError()

          // only send a single message at a time to the Total actor, backpressure otherwise
          val windSpeeds = measurements.map(each => each.measurements.wind_speed)
          (total ? Increment(measurements.size, average(windSpeeds), measurements.head.id))
            .mapTo[Done]
            .map(_ => lastMessage)
      }
      .map(Messages.ack) // ack the last message only


    val route =
      path("measurements" / JavaUUID) { id =>
        get {
          println(s"Receiving WindTurbineData form: $id")
          handleWebSocketMessages(measurementsWebSocketFlow)
        }
      }

    val httpInterface = "127.0.0.1"
    val httpPort = 8080

    logger.info(s"About ot bind to: $httpInterface and: $httpPort")
    val bindingFuture: Future[ServerBinding] = Http().newServerAt(httpInterface, httpPort).bindFlow(route)

    bindingFuture.map { serverBinding =>
      logger.info(s"Bound to: ${serverBinding.localAddress} ")
    }.onComplete {
      case Success(_) => logger.info("WindTurbineServer started successfully")
      case Failure(ex) =>
        logger.error("Failed to bind to {}:{}!", httpInterface, httpPort, ex)
        Http().shutdownAllConnectionPools()
        system.terminate()
    }

    scala.sys.addShutdownHook {
      logger.info("Terminating...")
      Http().shutdownAllConnectionPools()
      //actor system termination in 2.6.x is now implicit, see:
      //https://github.com/akka/akka/issues/28310
      logger.info("Terminated... Bye")
    }
  }

  /**
    * Generate server errors at 1/6 of the time
    * Clients will receive:
    * org.apache.pekko.http.scaladsl.model.ws.PeerClosedConnectionException: Peer closed connection with code 1011 'internal error'
    * and are able to recover due to the RestartSource used
    */
  private def generateRandomServerError(): Unit = {
    val time = LocalTime.now()
    if (time.getSecond > 50) {
      println(s"Server RuntimeException at: $time")
      throw new RuntimeException("BOOM - server RuntimeException")
    }
  }
}
