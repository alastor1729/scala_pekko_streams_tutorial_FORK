package akkahttp

import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.model.ws.*
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.stream.scaladsl.{BroadcastHub, Flow, Keep, MergeHub, Sink, Source}
import org.apache.pekko.{Done, NotUsed}

import scala.collection.parallel.CollectionConverters.*
import scala.concurrent.Future
import scala.concurrent.duration.*
/**
  * A simple WebSocket chat system using only pekko streams with the help of MergeHub Source and BroadcastHub Sink
  * See also: [[WebsocketEcho]]
  *
  * Initial version shamelessly stolen from:
  * https://github.com/calvinlfer/akka-http-streaming-response-examples/blob/master/src/main/scala/com/experiments/calvin/WebsocketStreamsMain.scala
  * Doc:
  * http://doc.akka.io/docs/akka/current/scala/stream/stream-dynamic.html#dynamic-fan-in-and-fan-out-with-mergehub-and-broadcasthub
  */
object WebsocketChatEcho extends App with ClientCommon {

    val (address, port) = ("127.0.0.1", 6002)
  // The heartbeat_echo endpoint is not implemented here
    chatServer(address, port)
    browserClient()
    val clients = List("Bob", "Alice")
    clients.par.foreach(clientName => clientWebSocketClientFlow(clientName, address, port))

  private def chatServer(address: String, port: Int) = {

   /*
  clients -> Merge Hub -> Broadcast Hub -> clients
  Visually
                                                                                                         Akka Streams Flow
               ________________________________________________________________________________________________________________________________________________________________________________________
  c1 ----->\  |                                                                                                                                                                                        |  /->----------- c1
            \ |                                                                                                                                                                                        | /
  c2 -------->| Sink ========================(feeds data to)===========> MergeHub Source ->-->-->--> BroadcastHub Sink ======(feeds data to)===========> Source                                        |->->------------ c2
             /| that comes from materializing the                                        connected to                                                    that comes from materializing the             | \
            / | MergeHub Source                                                                                                                          BroadcastHub Sink                             |  \
  c3 ----->/  |________________________________________________________________________________________________________________________________________________________________________________________|   \->---------- c3


  Runnable Flow (MergeHubSource -> BroadcastHubSink)

  Materializing a MergeHub Source yields a Sink that collects all the emitted elements and emits them in the MergeHub Source (the emitted elements that are collected in the Sink are coming from all WebSocket clients)
  Materializing a BroadcastHub Sink yields a Source that broadcasts all elements being collected by the MergeHub Sink (the elements that are emitted/broadcasted in the Source are going to all WebSocket clients)
   */

    // To demonstrate the nature of the composition
    val sampleProcessingFlow  = Flow[String].map(i => i.toUpperCase)

    val (inSink: Sink[String, NotUsed], outSource: Source[String, NotUsed]) = {
      MergeHub.source[String](1)
        //.wireTap(elem => logger.info(s"Server received after MergeHub: $elem"))
        .via(sampleProcessingFlow)
        .toMat(BroadcastHub.sink[String])(Keep.both).run()
    }

    val echoFlow: Flow[Message, Message, NotUsed] =
    Flow[Message].mapAsync(1) {
      case TextMessage.Strict(text) =>
        logger.info(s"Server received: $text")
        Future.successful(text)
      case TextMessage.Streamed(textStream) =>
        textStream.runReduce(_ + _).flatMap(Future.successful)
      case bm: BinaryMessage => throw new Exception(s"Binary message: $bm cannot be handled")
      case other => throw new Exception(s"Unhandled message type: $other cannot be handled")
      }
      .via(Flow.fromSinkAndSourceCoupled(inSink, outSource))
      // Optional msg aggregation
      .groupedWithin(10, 2.second)
      .map { eachSeq =>
        logger.info(s"Server aggregated: ${eachSeq.size} chat messages within 2 seconds")
        eachSeq.mkString("; ")
      }
      .map[Message](string => TextMessage.Strict("Hello " + string + "!"))

    def wsClientRoute: Route =
      path("echochat") {
        handleWebSocketMessages(echoFlow)
      }

    // The browser client has a different route but hooks into the same echoFlow
    def wsBrowserClientRoute: Route =
      path("echo") {
        handleWebSocketMessages(echoFlow)
      }

    def routes: Route = {
      wsClientRoute ~ wsBrowserClientRoute
    }

    val bindingFuture = Http().newServerAt(address, port).bindFlow(routes)
    bindingFuture
      .map(_.localAddress)
      .map(addr => logger.info(s"Server bound to: $addr"))
  }

  private def clientWebSocketClientFlow(clientName: String, address: String, port: Int): Unit = {

    val webSocketNonReusableFlow: Flow[Message, Message, Future[WebSocketUpgradeResponse]] = Http().webSocketClientFlow(WebSocketRequest(s"ws://$address:$port/echochat"))

    val (upgradeResponse, closed) =
      namedSource(clientName)
        .viaMat(webSocketNonReusableFlow)(Keep.right) // keep the materialized Future[WebSocketUpgradeResponse]
        .toMat(printSink)(Keep.both) // also keep the Future[Done]
        .run()


    val connected = upgradeResponse.flatMap { upgrade =>
      // status code 101 (Switching Protocols) indicates that server support WebSockets
      if (upgrade.response.status == StatusCodes.SwitchingProtocols) {
        Future.successful(Done)
      } else {
        throw new RuntimeException(s"Connection failed: ${upgrade.response.status}")
      }
    }

    connected.onComplete(_ => logger.info("client connected"))
    closed.foreach(_ => logger.info("client closed"))
  }
}
