package sample.stream

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.RestartSettings
import org.apache.pekko.stream.scaladsl.{Flow, Framing, Keep, RestartSource, Sink, Source, Tcp}
import org.apache.pekko.util.ByteString
import org.slf4j.{Logger, LoggerFactory}

import java.net.InetSocketAddress
import scala.collection.parallel.CollectionConverters.*
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.sys.process.*
import scala.util.{Failure, Success}

/**
  * TCP echo client server round trip
  * Use without parameters to start server and 100 parallel clients
  *
  * Use parameters `server 127.0.0.1 6000` to start server listening on port 6000
  *
  * Use parameters `client 127.0.0.1 6000` to start one client
  *
  * Run cmd line client:
  * echo -n "Hello World" | nc 127.0.0.1 6000
  *
  * Doc:
  * https://pekko.apache.org/docs/pekko/current/stream/stream-io.html?language=scala
  */
object TcpEcho extends App {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val systemServer = ActorSystem("TcpEchoServer")
  val systemClient = ActorSystem("TcpEchoClient")

  if (args.isEmpty) {
    val (host, port) = ("127.0.0.1", 6000)
    server(systemServer, host, port)

    // Issue: https://github.com/akka/akka/issues/29842
    checkResources()

    val maxClients = 100
    (1 to maxClients).par.foreach(each => client(each, systemClient, host, port))
  } else {
    val (host, port) =
      if (args.length == 3) (args(1), args(2).toInt)
      else ("127.0.0.1", 6000)
    if (args(0) == "server") {
      server(systemServer, host, port)
    } else if (args(0) == "client") {
      client(1, systemClient, host, port)
    }
  }

  def server(system: ActorSystem, host: String, port: Int): Future[Tcp.ServerBinding] = {
    implicit val sys: ActorSystem = system
    implicit val ec: ExecutionContextExecutor = system.dispatcher

    val handler = Sink.foreach[Tcp.IncomingConnection] { connection =>

      // parse incoming commands and append !
      val commandParser = Flow[String].takeWhile(_ != "BYE").map(_ + "!")

      val welcomeMsg = s"Welcome to: ${connection.localAddress}, you are: ${connection.remoteAddress}!"
      val welcomeSource = Source.single(welcomeMsg)

      val serverEchoFlow = Flow[ByteString]
        .via(Framing.delimiter( //chunk the inputs up into actual lines of text
          ByteString("\n"),
          maximumFrameLength = 256,
          allowTruncation = true))
        .map(_.utf8String)
        .via(commandParser)
        .merge(welcomeSource) // merge the initial banner after parser
        .map(_ + "\n")
        .map(ByteString(_))
        .watchTermination()((_, done) => done.onComplete {
          case Failure(err) =>
            logger.info(s"Server flow failed: $err")
          case _ => logger.info(s"Server flow terminated for client: ${connection.remoteAddress}")
        })
      connection.handleWith(serverEchoFlow)
    }

    val connections = Tcp().bind(interface = host, port = port)
    val binding = connections.watchTermination()(Keep.left).to(handler).run()

    binding.onComplete {
      case Success(b) =>
        logger.info("Server started, listening on: " + b.localAddress)
      case Failure(e) =>
        logger.info(s"Server could not bind to: $host:$port: ${e.getMessage}")
        system.terminate()
    }

    binding
  }

  def client(id: Int, system: ActorSystem, host: String, port: Int): Unit = {
    implicit val sys: ActorSystem = system
    implicit val ec: ExecutionContextExecutor = system.dispatcher

    // We want "halfClose behavior" on the client side. Doc:
    // https://github.com/akka/akka/issues/22163
    val connection: Flow[ByteString, ByteString, Future[Tcp.OutgoingConnection]] =
      Tcp().outgoingConnection(remoteAddress = InetSocketAddress.createUnresolved(host, port), halfClose = true)
    val testInput = ('a' to 'z').map(ByteString(_)) ++ Seq(ByteString("BYE"))
    logger.info(s"Client: $id sending: ${testInput.length} bytes")

    val restartSettings = RestartSettings(1.second, 10.seconds, 0.2).withMaxRestarts(10, 1.minute)
    val restartSource = RestartSource.onFailuresWithBackoff(restartSettings) { () => Source(testInput).via(connection) }
    val closed = restartSource.runForeach(each => logger.info(s"Client: $id received: ${each.utf8String}"))
    closed.onComplete(each => logger.info(s"Client: $id closed: $each"))
  }

  private def checkResources(): Unit = {
    // Depending on the available file descriptors of the OS we may experience client retries
    val os = System.getProperty("os.name").toLowerCase
    if (os == "mac os x") {
      val fileDesr = "launchctl limit maxfiles".!!
      logger.info(s"Running with: $fileDesr")
    }
  }
}
