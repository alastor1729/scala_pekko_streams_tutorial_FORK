package alpakka.env

import com.github.blemale.scaffeine.{Cache, Scaffeine}
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.StatusCodes.*
import org.apache.pekko.http.scaladsl.model.{HttpResponse, MediaTypes, StatusCodes}
import org.apache.pekko.http.scaladsl.server.Directives.{logRequestResult, path, *}
import org.apache.pekko.http.scaladsl.server.{ExceptionHandler, Route}
import org.slf4j.{Logger, LoggerFactory}

import java.io.File
import scala.concurrent.duration.*
import scala.util.{Failure, Success}

/**
  * HTTP FileServer to test: [[sample.stream_shared_state.LocalFileCacheCaffeine]]
  * Simulates a quirky legacy file download server encountered in real life
  *
  * The client can request these types of response:
  *  - HTTP 200 response:        /download/[id]
  *  - Flaky response:           /downloadflaky/[id]
  *  - Non-idempotent response:  /downloadni/[id]
  *    Allows only one download file request per id, answer with HTTP 404 on subsequent requests
  *
  * Uses a cache to remember the "one download per id" behaviour
  * Note that akka-http also supports server-side caching (by wrapping caffeine in caching directives):
  * https://doc.akka.io/docs/akka-http/current/routing-dsl/directives/caching-directives/index.html
  */
object FileServer extends App {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system: ActorSystem = ActorSystem()

  import system.dispatcher

  val (address, port) = ("127.0.0.1", 6001)
  server(address, port)

  def server(address: String, port: Int): Unit = {
    val resourceFileName = "payload.zip"

    val cache: Cache[String, String] =
      Scaffeine()
        .recordStats()
        .expireAfterWrite(1.hour)
        .maximumSize(500)
        .build[String, String]()


    val exceptionHandler = ExceptionHandler {
      case ex: RuntimeException =>
        extractUri { uri =>
          logger.error(s"Request to $uri could not be handled normally message: ${ex.getMessage}")
          //cache.invalidate(id)
          complete(HttpResponse(InternalServerError, entity = "Runtime ex occurred"))
        }
    }

    def routes: Route = handleExceptions(exceptionHandler) {
      logRequestResult("FileServer") {
        path("download" / Segment) { id =>
          logger.info(s"TRACE_ID: $id Server received download request")
          get {
            // for testing: use the same file, independent of the TRACE_ID
            getFromFile(new File(getClass.getResource(s"/$resourceFileName").toURI), MediaTypes.`application/zip`)
          }
        } ~ path("downloadflaky" / Segment) { id =>
          logger.info(s"TRACE_ID: $id Server received flaky download request")
          get {
            if (id.toInt % 10 == 0) { // 10, 20, 30
              complete(randomErrorHttpStatusCode)
            } else if (id.toInt % 5 == 0) { // 5, 15, 25
              // Causes TimeoutException on client if sleep time > 5 sec
              randomSleeper()
              getFromFile(new File(getClass.getResource(s"/$resourceFileName").toURI), MediaTypes.`application/zip`)
            } else {
              getFromFile(new File(getClass.getResource(s"/$resourceFileName").toURI), MediaTypes.`application/zip`)
            }
          }
        } ~ path("downloadni" / Segment) { id =>
          logger.info(s"TRACE_ID: $id Server received non-idempotent request")

          if (cache.getIfPresent(id).isDefined) {
            logger.warn(s"TRACE_ID: $id Only one download file request per TRACE_ID allowed. Reply with 404")
            complete(StatusCodes.NotFound)

          } else {
            cache.put(id, "downloading") // to simulate blocking on concurrent requests
            get {
              randomSleeper()
              val response = getFromFile(new File(getClass.getResource(s"/$resourceFileName").toURI), MediaTypes.`application/zip`)
              cache.put(id, "downloaded")
              response
            }
          }
        }
      }
    }

    val bindingFuture = Http().newServerAt(address, port).bindFlow(routes)
    bindingFuture.onComplete {
      case Success(b) =>
        logger.info(s"Server started, listening on: ${b.localAddress}")
      case Failure(e) =>
        logger.info(s"Server could not bind to $address:$port. Exception message: ${e.getMessage}")
        system.terminate()
    }
  }

  def randomSleeper(): Unit = {
    val (start, end) = (1000, 10000)
    val rnd = new scala.util.Random
    val sleepTime = start + rnd.nextInt((end - start) + 1)
    logger.debug(s" -> Sleep for $sleepTime ms")
    Thread.sleep(sleepTime.toLong)
  }

  def randomErrorHttpStatusCode = {
    val statusCodes = Seq(StatusCodes.InternalServerError, StatusCodes.BadRequest, StatusCodes.ServiceUnavailable)
    val start = 0
    val end = statusCodes.size - 1
    val rnd = new scala.util.Random
    val finalRnd = start + rnd.nextInt((end - start) + 1)
    val statusCode = statusCodes(finalRnd)
    logger.info(s" -> Complete with HTTP status code: $statusCode")
    statusCodes(finalRnd)
  }
}