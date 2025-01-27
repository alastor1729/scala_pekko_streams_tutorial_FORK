package alpakka.tcp_to_websockets.websockets

import alpakka.tcp_to_websockets.websockets.WebsocketClientActor.{Connected, ConnectionFailure}
import org.apache.pekko.Done
import org.apache.pekko.actor.{ActorRef, ActorSystem}
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.model.ws.*
import org.apache.pekko.stream.scaladsl.{Flow, Keep, Sink, Source, SourceQueue}
import org.apache.pekko.stream.{OverflowStrategy, QueueOfferResult}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}

object WebSocketClient {
  def apply(id: String, endpoint: String, websocketClientActor: ActorRef)
           (implicit
            system: ActorSystem,
            executionContext: ExecutionContext): WebSocketClient = {
    new WebSocketClient(id, endpoint, websocketClientActor)(system, executionContext)
  }
}

class WebSocketClient(id: String, endpoint: String, websocketClientActor: ActorRef)
                     (implicit
                      system: ActorSystem,
                      executionContext: ExecutionContext) {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  val printSink: Sink[Message, Future[Done]] = createEchoPrintSink()
  val sourceQueue: Future[SourceQueue[Message]] = singleWebSocketRequestSourceQueueClient(id, endpoint)


  def singleWebSocketRequestSourceQueueClient(id: String, endpoint: String): Future[SourceQueue[Message]] = {

    val (source, sourceQueue) = {
      val p = Promise[SourceQueue[Message]]()
      val s = Source.queue[Message](0, OverflowStrategy.backpressure, 1).mapMaterializedValue(m => {
        p.trySuccess(m)
        m
      })
      (s, p.future)
    }

    val webSocketNonReusableFlow = Flow.fromSinkAndSourceMat(printSink, source)(Keep.right)

    val (upgradeResponse, sourceQueueWithComplete) =
      Http().singleWebSocketRequest(WebSocketRequest(endpoint), webSocketNonReusableFlow)

    val connected = handleUpgrade(upgradeResponse)

    connected.onComplete((done: Try[Done.type]) => {
      done match {
        case Success(_) =>
          websocketClientActor ! Connected
        case Failure(ex) =>
          websocketClientActor ! ConnectionFailure(ex)
      }
    })
    sourceQueueWithComplete.watchCompletion().onComplete((closed: Try[Done]) => {
      closed match {
        case Success(_) =>
          logger.info(s"Client $id: closed: $closed")
          websocketClientActor ! ConnectionFailure(new RuntimeException("Closed!"))
        case Failure(ex) =>
          logger.info(s"Client $id: closed: $closed")
          websocketClientActor ! ConnectionFailure(ex)
      }
    })
    sourceQueue
  }


  private def handleUpgrade(upgradeResponse: Future[WebSocketUpgradeResponse]) = {
    upgradeResponse.map { upgrade =>
      // status code 101 (Switching Protocols) indicates that server support WebSockets
      if (upgrade.response.status == StatusCodes.SwitchingProtocols) {
        Done
      } else {
        throw new RuntimeException(s"Connection failed: ${upgrade.response.status}")
      }
    }
  }

  def sendToWebsocket(messageText: String): Future[Unit] = {
    val message = TextMessage.Strict(messageText)
    sourceQueue.flatMap { queue =>
      queue.offer(message: Message).map {
        case QueueOfferResult.Enqueued => logger.info(s"Enqueued: ${printableShort(message.text)}")
        case QueueOfferResult.Dropped => logger.info(s"Dropped: ${printableShort(message.text)}")
        case QueueOfferResult.Failure(ex) => logger.info(s"Offer failed: $ex")
        case QueueOfferResult.QueueClosed => logger.info("Source queue closed")
      }
    }
  }


  private def createEchoPrintSink(): Sink[Message, Future[Done]] = {
    Sink.foreach {
      //see https://github.com/akka/akka-http/issues/65
      case TextMessage.Strict(text) => logger.info(s"WebSocket client received ACK TextMessage.Strict: ${printableShort(text)}")
      case TextMessage.Streamed(textStream) => textStream.runFold("")(_ + _).onComplete { value =>
        logger.info(s"WebSocket client received TextMessage.Streamed: ${printableShort(value.get)}")
      }
      case BinaryMessage.Strict(binary) => //do nothing
      case BinaryMessage.Streamed(binaryStream) => binaryStream.runWith(Sink.ignore)
    }
  }

  // The HAPI parser needs /r as segment terminator, but this is not printable
  private def printable(message: String): String = {
    message.replace("\r", "\n")
  }

  private def printableShort(message: String): String = {
    printable(message).take(20).concat("...")
  }
}
