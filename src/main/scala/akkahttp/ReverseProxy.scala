package akkahttp

import akkahttp.ReverseProxy.Mode.Mode
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import io.circe.*
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.model.*
import org.apache.pekko.http.scaladsl.model.Uri.Authority
import org.apache.pekko.http.scaladsl.model.headers.{Host, RawHeader}
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.http.scaladsl.settings.ServerSettings
import org.apache.pekko.http.scaladsl.{Http, HttpExt}
import org.apache.pekko.pattern.{CircuitBreaker, CircuitBreakerOpenException}
import org.apache.pekko.stream.ThrottleMode
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.parallel.CollectionConverters.ImmutableIterableIsParallelizable
import scala.concurrent.*
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success}

/**
  * This conceptual all-in-one PoC is inspired by:
  * https://github.com/mathieuancelin/akka-http-reverse-proxy
  *
  * Features ReverseProxy:
  *  - Weighted round robin load balancing
  *  - Retry on HTTP 5xx from target servers
  *  - CircuitBreaker per target server to avoid overload
  *  - HTTP Header `X-Correlation-ID` for tracing (only for Mode.local)
  *
  * Mode.local:
  * HTTP client(s) --> ReverseProxy --> local target server(s)
  *
  * Mode.remote:
  * HTTP client(s) --> ReverseProxy --> remote target server(s)
  *
  * Remarks:
  *  - The target server selection is via the "Host" HTTP header
  *  - Local/Remote target servers are designed to be flaky to show Retry/CircuitBreaker behavior
  *  - On top of the built in client, you may also try other clients
  *  - This PoC may not scale well, possible bottlenecks are:
  *     - Combination of Retry/CircuitBreaker
  *     - Round robin impl. with `requestCounter` means shared state
  *
  * Gatling client: [[ReverseProxySimulation]]
  *
  * curl client:
  * curl -H "Host: local" -H "X-Correlation-ID: 1-1" -o - -i -w " %{time_total}\n" http://127.0.0.1:8080/mypath
  * curl -H "Host: remote" -o - -i -w " %{time_total}\n" http://127.0.0.1:8080/200
  *
  * wrk perf client:
  * wrk -t2 -c10 -d10s -H "Host: local" --latency http://127.0.0.1:8080/mypath
  * wrk -t2 -c10 -d10s -H "Host: remote" --latency http://127.0.0.1:8080/200
  *
  * Doc:
  * https://pekko.apache.org/docs/pekko/current/common/circuitbreaker.html
  * https://pekko.apache.org/docs/pekko-http/current//implications-of-streaming-http-entity.html
  * https://pekko.apache.org/docs/pekko-http/current///common/timeouts.html#request-timeout
  */
object ReverseProxy extends App {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system: ActorSystem = ActorSystem()

  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  implicit val http: HttpExt = Http(system)

  val circuitBreakers = new ConcurrentHashMap[String, CircuitBreaker]()
  val requestCounter = new AtomicInteger(0)

  val proxyHost = "127.0.0.1"
  val proxyPort = 8080

  val services: Map[Mode, Seq[Target]] = Map(
    Mode.local -> Seq(
      Target.weighted("http://127.0.0.1:9081", 1),
      Target.weighted("http://127.0.0.1:9082", 2),
      Target.weighted("http://127.0.0.1:9083", 3)
    ),
    Mode.remote -> Seq(
      Target.weighted("https://httpstat.us:443", 1),
      Target.weighted("https://httpstat.us:443", 2),
      Target.weighted("https://httpstat.us:443", 3)
    )
  )

  localTargetServers(maxConnections = 5) // 1-1024
  reverseProxy()
  // Switch here to force ReverseProxy to forward requests to local or remote target server(s)
  clients(nbrOfClients = 10, requestsPerClient = 10, Mode.local)

  // HTTP client(s)
  def clients(nbrOfClients: Int = 1, requestsPerClient: Int = 1, mode: Mode): Unit = {
    logger.info(s"Running $nbrOfClients client(s), each sending $requestsPerClient requests")
    val clients = 1 to nbrOfClients
    clients.par.foreach(clientID => httpClient(clientID, proxyHost, proxyPort, mode, requestsPerClient))

    def httpClient(clientId: Int, proxyHost: String, proxyPort: Int, targetHost: Mode, nbrOfRequests: Int) = {
      def logResponse(response: HttpResponse): Unit = {
        val id = response.getHeader("X-Correlation-ID").orElse(RawHeader("X-Correlation-ID", "N/A")).value()
        val msg = response.entity.dataBytes.runReduce(_ ++ _).map(data => data.utf8String)
        msg.onComplete(msg => logger.info(s"Client: $clientId got response: ${response.status.intValue()} for id: $id and msg: ${msg.getOrElse("N/A")}"))
      }

      val fixedPath = mode match {
        case Mode.local => ""
        case Mode.remote => "random/200,201,500-504"
      }

      Source(1 to nbrOfRequests)
        .throttle(1, 1.second, 10, ThrottleMode.shaping)
        .wireTap(each => logger.info(s"Client: $clientId about to send request with id: $clientId-$each..."))
        .mapAsync(1)(each => http.singleRequest(HttpRequest(uri = s"http://$proxyHost:$proxyPort/$fixedPath")
          .withHeaders(Seq(RawHeader("Host", targetHost.toString), RawHeader("X-Correlation-ID", s"$clientId-$each")))))
        .wireTap(response => logResponse(response))
        .runWith(Sink.ignore)
    }
  }

  // ReverseProxy server
  def reverseProxy(): Unit = {
    def NotFound(id: String, path: String) = HttpResponse(
      404,
      entity = HttpEntity(ContentTypes.`application/json`, Json.obj("error" -> Json.fromString(s"$path not found")).noSpaces)
    ).withHeaders(Seq(RawHeader("X-Correlation-ID", id)))

    def GatewayTimeout(id: String) = HttpResponse(
      504,
      entity = HttpEntity(ContentTypes.`application/json`, Json.obj("error" -> Json.fromString(s"Target server timeout")).noSpaces)
    ).withHeaders(Seq(RawHeader("X-Correlation-ID", id)))

    def BadGateway(id: String, message: String) = HttpResponse(
      502,
      entity = HttpEntity(ContentTypes.`application/json`, Json.obj("error" -> Json.fromString(message)).noSpaces)
    ).withHeaders(Seq(RawHeader("X-Correlation-ID", id)))

    def handlerWithCircuitBreaker(request: HttpRequest): Future[HttpResponse] = {
      val host = request.header[Host].map(_.host.address()).getOrElse("N/A")
      val mode = Mode.values.find(_.toString == host).getOrElse(Mode.local)
      val id = request.getHeader("X-Correlation-ID").orElse(RawHeader("X-Correlation-ID", "N/A")).value()

      def headers(target: Target) = {
        val headersIn: Seq[HttpHeader] =
          request.headers.filterNot(t => t.name() == "Host") :+
            Host(target.host, target.port) :+
            RawHeader("X-Forwarded-Host", host) :+
            RawHeader("X-Forwarded-Scheme", request.uri.scheme) :+
            RawHeader("X-Correlation-ID", id)

        // Filter, to avoid log noise, see: https://github.com/akka/akka-http/issues/64
        val filteredHeaders = headersIn.toList.filterNot(each => each.name() == "Timeout-Access")
        filteredHeaders
      }

      def uri(target: Target) = {
        val uri: Uri = request.uri.copy(
          scheme = target.scheme,
          authority = Authority(host = Uri.NamedHost(target.host), port = target.port))
        uri
      }

      services.get(mode) match {
        case Some(rawSeq) =>
          val seq = rawSeq.flatMap(t => (1 to t.weight).map(_ => t))
          Retry.retry[HttpResponse](times = 3) {
            val index = requestCounter.incrementAndGet() % (if (seq.isEmpty) 1 else seq.size)
            val target = seq(index)
            logger.info(s"Forwarding request with id: $id to $mode target server: ${target.url}")
            val circuitBreaker = circuitBreakers.computeIfAbsent(target.url, _ => new CircuitBreaker(
              system.scheduler,
              // A low value opens the circuit breaker for subsequent requests (until resetTimeout)
              maxFailures = 2,
              // Needs to be shorter than pekko-http 'request-timeout' (20s)
              // If not, clients get 503 from pekko-http
              callTimeout = 10.seconds,
              resetTimeout = 10.seconds))
            val proxyReq = request.withUri(uri(target)).withHeaders(headers(target))
            circuitBreaker.withCircuitBreaker(http.singleRequest(proxyReq))
          }.recover {
            case _: CircuitBreakerOpenException => BadGateway(id, "Circuit breaker opened")
            case _: TimeoutException => GatewayTimeout(id)
            case e => BadGateway(id, e.getMessage)
          }
        case None => Future.successful(NotFound(id, host))
      }
    }

    val futReverseProxy = Http().newServerAt(proxyHost, proxyPort).bind(handlerWithCircuitBreaker)

    futReverseProxy.onComplete {
      case Success(b) =>
        logger.info("ReverseProxy started, listening on: " + b.localAddress)
      case Failure(e) =>
        logger.info(s"ReverseProxy failed. Exception message: ${e.getMessage}")
        system.terminate()
    }
  }

  // Local target servers (with faulty behaviour and throttled)
  def localTargetServers(maxConnections: Int): Unit = {
    val echoRoute: Route =
      extractRequest { request =>
        complete {
          Thread.sleep(500)
          val id = request.getHeader("X-Correlation-ID").orElse(RawHeader("X-Correlation-ID", "N/A")).value()

          // Adjust to provoke more retries on ReverseProxy
          val codes = List(200, 200, 200, 500, 500, 500)
          val randomResponse = codes(new scala.util.Random().nextInt(codes.length))
          logger.info(s"Target server: ${request.uri.authority.host}:${request.uri.effectivePort} got echo request with id: $id, reply with: $randomResponse")
          (StatusCode.int2StatusCode(randomResponse), Seq(RawHeader("X-Correlation-ID", id)))
        }
      }

    services.get(Mode.local).foreach(targetSeq =>
      targetSeq.foreach(target => {

        // Tweaked config to throttle target servers
        val tweakedConf = ConfigFactory.empty()
          .withValue("pekko.http.server.max-connections", ConfigValueFactory.fromAnyRef(maxConnections))
          .withFallback(ConfigFactory.load())
        val serverSettings = ServerSettings(tweakedConf)
        val futTargetServer = Http().newServerAt(target.host, target.port)
          .withSettings(serverSettings)
          .bind(echoRoute)

        futTargetServer.onComplete {
          case Success(b) =>
            logger.info(s"Local target server started, listening on: ${b.localAddress}")
          case Failure(e) =>
            logger.info(s"Local target server could not bind to... Exception message: ${e.getMessage}")
            system.terminate()
        }
      }
      )
    )
  }

  // Models
  case class Target(scheme: String, host: String, port: Int, weight: Int = 1, protocol: HttpProtocol = HttpProtocols.`HTTP/1.1`) {
    def url: String = s"$scheme://$host:$port"
  }

  object Target {
    def apply(url: String): Target = {
      url.split("://|:").toList match {
        case scheme :: host :: port :: Nil => Target(scheme, host, port.toInt)
        case _ => throw new RuntimeException(s"Can not resolve target url: $url.")
      }
    }

    // weight: 1-n, higher value means more requests will reach this server
    def weighted(url: String, weight: Int): Target = {
      Target(url).copy(weight = weight)
    }
  }

  object Mode extends Enumeration {
    type Mode = Value
    val local, remote = Value
  }
}

object Retry {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def retry[T](times: Int)(f: => Future[T])(implicit ec: ExecutionContext): Future[T] = {
    val promise = Promise[T]()
    retryPromise[T](times, promise, None, f)
    promise.future
  }

  private[this] def retryPromise[T](times: Int, promise: Promise[T], failure: Option[Throwable],
                                    f: => Future[T])(implicit ec: ExecutionContext): Unit = {
    (times, failure) match {
      case (0, Some(e)) => promise.tryFailure(e)
      case (0, None) => promise.tryFailure(new RuntimeException("Failure, but lost track of exception"))
      case (_, _) =>
        f.onComplete {
          case Success(httpResponse: HttpResponse) if httpResponse.status.intValue() >= 500 =>
            val id = httpResponse.getHeader("X-Correlation-ID").orElse(RawHeader("X-Correlation-ID", "N/A")).value()
            logger.info(s"ReverseProxy got 5xx server error for id: $id. Retries left: ${times - 1}")
            retryPromise[T](times - 1, promise, Some(new RuntimeException(s"Received: ${httpResponse.status.intValue()} from target server")), f)
          case Success(t) => promise.trySuccess(t)
          case Failure(e) => retryPromise[T](times - 1, promise, Some(e), f)
        }
    }
  }
}