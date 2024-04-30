package akkahttp

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.common.{EntityStreamingSupport, JsonEntityStreamingSupport}
import org.apache.pekko.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import org.apache.pekko.http.scaladsl.model.*
import org.apache.pekko.http.scaladsl.server.Directives.{complete, get, logRequestResult, path, *}
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.http.scaladsl.unmarshalling.Unmarshal
import org.apache.pekko.stream.scaladsl.{Flow, Sink, Source}
import org.slf4j.{Logger, LoggerFactory}
import sample.graphstage.StreamEventInspector
import spray.json.{DefaultJsonProtocol, RootJsonFormat}

import scala.concurrent.Future
import scala.concurrent.duration.*
import scala.util.{Failure, Success}

/**
  * Initiate n singleRequest and in the response consume the stream of elements from the server
  * From client point of view similar to [[alpakka.sse.SSEHeartbeat]]
  *
  * Doc streaming implications:
  * https://doc.akka.io/docs/akka-http/current/implications-of-streaming-http-entity.html#implications-of-the-streaming-nature-of-request-response-entities
  *
  * Doc JSON streaming support:
  * https://doc.akka.io/docs/akka-http/current/routing-dsl/source-streaming-support.html
  * https://doc.akka.io/docs/akka-http/current/common/json-support.html
  *
  * Remarks:
  *  - No retry logic
  *
  */
object HTTPResponseStream extends App with DefaultJsonProtocol with SprayJsonSupport {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system: ActorSystem = ActorSystem()

  import system.dispatcher

  final case class Person(name: String)

  private object Person extends Serializable {
    implicit val personFormat: RootJsonFormat[Person] = jsonFormat1(Person.apply)
  }
  
  implicit val jsonStreamingSupport: JsonEntityStreamingSupport = EntityStreamingSupport.json()

  val (address, port) = ("127.0.0.1", 8080)
  server(address, port)
  client(address, port)

  def client(address: String, port: Int): Unit = {
    val requestParallelism = 2

    val requests = Source
      .fromIterator(() =>
        Range(0, requestParallelism)
          .map(i => HttpRequest(uri = Uri(s"http://$address:$port/download/$i")))
          .iterator
      )

    // Run singleRequest and then consume all response elements
    def runRequestDownload(req: HttpRequest) =
      Http()
        .singleRequest(req)
        .flatMap { response =>
          val unmarshalled: Future[Source[Person, NotUsed]] = Unmarshal(response).to[Source[Person, NotUsed]]
          val source: Source[Person, Future[NotUsed]] = Source.futureSource(unmarshalled)
          source.via(processorFlow).runWith(printSink)
        }

    requests
      .mapAsync(requestParallelism)(runRequestDownload)
      .runWith(Sink.ignore)
  }


  val printSink = Sink.foreach[Person] { each => logger.info(s"Client processed element: $each") }

  val processorFlow: Flow[Person, Person, NotUsed] = Flow[Person].map {
    each => {
      //logger.info(s"Process: $each")
      each
    }
  }


  def server(address: String, port: Int): Unit = {

    def routes: Route = logRequestResult("httpecho") {
      path("download" / Segment) { clientId =>
        get {
          logger.info(s"Server received request with id: $clientId, start streaming response...")
          extractRequest { httpRequest =>
            val finishedWriting = httpRequest.discardEntityBytes().future
            onComplete(finishedWriting) { _ =>

              val numberOfMessages = 100
              val response = Source
                .tick(1.second, 100.millis, ())
                .zipWith(Source(1 to numberOfMessages))((_, nbr) => Person(s"$clientId-$nbr"))
                // Optional, eg for debugging
                .via(StreamEventInspector(httpRequest.uri.path.toString(), Person => Person.toString))
              complete(response)
            }
          }
        }
      }
    }

    val bindingFuture = Http().newServerAt(address, port).bindFlow(routes)
    bindingFuture.onComplete {
      case Success(b) =>
        logger.info("Server started, listening on: " + b.localAddress)
      case Failure(e) =>
        logger.info(s"Server could not bind to: $address:$port. Exception message: ${e.getMessage}")
        system.terminate()
    }
  }
}