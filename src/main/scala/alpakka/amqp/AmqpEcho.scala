package alpakka.amqp

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.connectors.amqp.*
import org.apache.pekko.stream.connectors.amqp.scaladsl.*
import org.apache.pekko.stream.scaladsl.{Flow, Keep, RestartFlow, Sink, Source}
import org.apache.pekko.stream.{KillSwitches, RestartSettings, ThrottleMode}
import org.apache.pekko.util.ByteString
import org.apache.pekko.{Done, NotUsed}
import org.slf4j.{Logger, LoggerFactory}
import org.testcontainers.containers.RabbitMQContainer

import scala.collection.parallel.CollectionConverters.*
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Future, Promise}
import scala.sys.process.{Process, stringSeqToProcess}
import scala.util.{Failure, Random, Success}

/**
  * Inspired by:
  * https://doc.akka.io/docs/alpakka/current/amqp.html
  *
  */
object AmqpEcho extends App {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system: ActorSystem = ActorSystem()

  import system.dispatcher

  val (host, port) = ("127.0.0.1", 5672)
  val queueName = "queue"

  val rabbitMQContainer = new RabbitMQContainer("rabbitmq:management")
  rabbitMQContainer.start()
  logger.info(s"Started RabbitMQ on: ${rabbitMQContainer.getHost}:${rabbitMQContainer.getMappedPort(port)}")

  (1 to 2).par.foreach(each => sendReceiveClient(each, rabbitMQContainer))
  (1 to 2).par.foreach(each => pubSubClient(each, rabbitMQContainer))
  (1 to 2).par.foreach(each => rpcScenario(each, rabbitMQContainer))

  def sendReceiveClient(id: Int, rabbitMQContainer: RabbitMQContainer): Unit = {
    val mappedPort = rabbitMQContainer.getAmqpPort
    val amqpUri = s"amqp://$host:$mappedPort"
    val connectionProvider = AmqpCachedConnectionProvider(AmqpUriConnectionProvider(amqpUri))

    val queueNameFull = s"$queueName-$id"
    val queueDeclaration = QueueDeclaration(queueNameFull)

    sendToQueue(id, connectionProvider, queueDeclaration, queueNameFull)
      .onComplete {
        case Success(writeResult) =>
          val noOfSentMsg = writeResult.size
          logger.info(s"Client: $id sent: $noOfSentMsg messages to queue: $queueNameFull. Starting receiver...")
          receiveFromQueueAck(id, connectionProvider, queueDeclaration, noOfSentMsg, queueNameFull)
        case Failure(exception) => logger.info(s"Exception during send:", exception)
      }
  }

  /**
    * Send messages to an "exchange" and then provide instructions to the AMQP server
    * what to do with these incoming messages. The "fanout" type of the exchange
    * enables message broadcasting to multiple consumers.
    *
    * @param id
    * @param rabbitMQContainer
    */
  def pubSubClient(id: Int, rabbitMQContainer: RabbitMQContainer): Unit = {
    val connectionProvider =
      AmqpCachedConnectionProvider(
        AmqpDetailsConnectionProvider(
          host, rabbitMQContainer.getAmqpPort
        )
          // see: https://github.com/akka/alpakka/issues/1270
          .withAutomaticRecoveryEnabled(false)
          .withTopologyRecoveryEnabled(false)
          .withNetworkRecoveryInterval(10)
          .withRequestedHeartbeat(10)
      )

    val exchangeName = s"exchange-pub-sub-$id"
    val exchangeDeclaration = ExchangeDeclaration(exchangeName, "fanout")

    receiveFromExchange(id, connectionProvider, exchangeName, exchangeDeclaration)
    sendToExchange(id, connectionProvider, exchangeName, exchangeDeclaration)
  }

  /**
    * Show the AMQP 'RPC scenario', see diagram:
    * https://www.rabbitmq.com/tutorials/tutorial-six-java.html
    * Inspired by:
    * https://stackoverflow.com/questions/76089253/amqprpc-alpakka-producer-not-receiving-a-response-back
    *
    * @param id
    * @param rabbitMQContainer
    */
  def rpcScenario(id: Int, rabbitMQContainer: RabbitMQContainer): Unit = {
    val mappedPort = rabbitMQContainer.getAmqpPort
    val amqpUri = s"amqp://$host:$mappedPort"
    val connectionProvider = AmqpCachedConnectionProvider(AmqpUriConnectionProvider(amqpUri))

    val queueNameRPC = "rpc"
    val queueNameFullRPC = s"$queueNameRPC-$id"
    val queueDeclarationRPC = QueueDeclaration(queueNameFullRPC)

    // RPC server flow
    val rpcFlow: Flow[ReadResult, WriteMessage, NotUsed] = Flow[ReadResult]
      .map { readResult =>
        logger.info(s"RECEIVED on server envelope: ${readResult.envelope}")
        val output = s"Processed: ${readResult.bytes.utf8String}"
        // The on-the-fly created replyTo queue name is in the properties
        WriteMessage(ByteString(output)).withProperties(readResult.properties)
      }

    val amqpSink: Sink[WriteMessage, Future[Done]] = AmqpSink.replyTo(
      AmqpReplyToSinkSettings(connectionProvider)
    )

    val amqpSource: Source[ReadResult, NotUsed] = AmqpSource.atMostOnceSource(
      NamedQueueSourceSettings(connectionProvider, queueNameFullRPC)
        .withDeclaration(queueDeclarationRPC),
      bufferSize = 1
    )

    val doneServer: Future[Done] = amqpSource
      .via(rpcFlow)
      .runWith(amqpSink)

    doneServer.onComplete {
      case Success(_) => logger.info("Done RPC server flow")
      case Failure(exception) => logger.info(s"Exception during RPC server flow: ", exception)
    }

    // RPC client flow
    val inputMessages: Source[WriteMessage, NotUsed] = Source(List(
      WriteMessage(ByteString("one")),
      WriteMessage(ByteString("two")),
      WriteMessage(ByteString("three")),
      WriteMessage(ByteString("four")),
      WriteMessage(ByteString("five")),
    ))

    val amqpRpcFlow: Flow[WriteMessage, ReadResult, Future[String]] = AmqpRpcFlow.atMostOnceFlow(
      AmqpWriteSettings(connectionProvider)
        .withRoutingKey(queueNameFullRPC)
        .withDeclaration(queueDeclarationRPC)
        .withBufferSize(10)
        .withConfirmationTimeout(200.millis),
      10
    )

    val printSink = Sink.foreach[ReadResult](each => logger.info(s"Reached sink: ${each.bytes.utf8String}"))
    val done: (Future[String], Future[Done]) = inputMessages
      .wireTap(each => logger.info("Sending: " + each))
      .viaMat(amqpRpcFlow)(Keep.right)
      .toMat(printSink)(Keep.both)
      .run()

    done._2.onComplete {
      case Success(_) => logger.info("Done RPC client flow")
      case Failure(exception) => logger.info(s"Exception during RPC client flow:", exception)
    }

    done._1.onComplete {
      case Success(queue) => logger.info(s"Internally created REPLY-TO queue was: $queue")
      case Failure(exception) => logger.info(s"Exception during RPC client flow:", exception)
    }
  }

  private def sendToQueue(id: Int, connectionProvider: AmqpCachedConnectionProvider, queueDeclaration: QueueDeclaration, queueNameFull: String) = {
    logger.info(s"Starting sendToQueue: $queueNameFull...")

    val settings = AmqpWriteSettings(connectionProvider)
      .withRoutingKey(queueNameFull)
      .withDeclaration(queueDeclaration)
      .withBufferSize(10)
      .withConfirmationTimeout(200.millis)

    val amqpFlow: Flow[WriteMessage, WriteResult, Future[Done]] =
      AmqpFlow.withConfirm(settings)

    val restartSettings = RestartSettings(1.second, 10.seconds, 0.2).withMaxRestarts(10, 1.minute)
    val restartFlow = RestartFlow.onFailuresWithBackoff(restartSettings)(() => amqpFlow)

    val writeResult: Future[Seq[WriteResult]] =
      Source(1 to 10)
        .map(each => WriteMessage(ByteString(s"$id-$each")))
        .via(restartFlow)
        .wireTap(each => logger.debug(s"WriteResult: $each"))
        .runWith(Sink.seq)
    writeResult
  }

  private def receiveFromQueueAck(id: Int, connectionProvider: AmqpCachedConnectionProvider, queueDeclaration: QueueDeclaration, noOfSentMsg: Int, queueNameFull: String): Unit = {
    logger.info(s"Starting receiveFromQueueAck: $queueNameFull...")

    val amqpSource = AmqpSource.committableSource(
      NamedQueueSourceSettings(connectionProvider, queueNameFull)
        .withDeclaration(queueDeclaration)
        .withAckRequired(true),
      bufferSize = 10
    )

    val done = amqpSource
      .mapAsync(1)(cm => simulateRandomIssueWhileProcessing(cm))
      .collect { case Some(readResult) => readResult }
      .wireTap(each => logger.info(s"Client: $id received and ACKed msg: ${each.bytes.utf8String} from queue: $queueNameFull"))
      .runWith(Sink.ignore)

    done.onComplete {
      case Success(_) => logger.info("Receive loop is done")
      case Failure(exception) => logger.info(s"Exception during receive:", exception)
    }
  }

  private def simulateRandomIssueWhileProcessing(cm: CommittableReadResult) = {
    val payloadParsed = cm.message.bytes.utf8String.split("-").last.toInt

    if (payloadParsed % 2 == Random.nextInt(2)) {
      logger.info(s"Processing OK  - reply with ACK: $payloadParsed")
      cm.ack().map(_ => Some(cm.message))
    } else {
      // Reject the message and ask server to re-queue (= place to its original position, if possible)
      logger.warn(s"Processing NOK - reply with NACK: $payloadParsed")
      cm.nack(multiple = false, requeue = true).map(_ => None)
    }
  }

  private def sendToExchange(id: Int, connectionProvider: AmqpCachedConnectionProvider, exchangeName: String, exchangeDeclaration: ExchangeDeclaration): Unit = {
    // Wait until the receiver has registered
    Thread.sleep(1000)
    logger.info(s"Starting sendToExchange: $exchangeName...")

    val settings = AmqpWriteSettings(connectionProvider)
      .withExchange(exchangeName)
      .withDeclaration(exchangeDeclaration)
      .withBufferSize(10)
      .withConfirmationTimeout(200.millis)

    val amqpFlow: Flow[WriteMessage, WriteResult, Future[Done]] =
      AmqpFlow.withConfirm(settings)

    val restartSettings = RestartSettings(1.second, 10.seconds, 0.2).withMaxRestarts(10, 1.minute)
    val restartFlow = RestartFlow.onFailuresWithBackoff(restartSettings)(() => amqpFlow)

    val done: Future[Done] = Source(1 to 10)
      .throttle(1, 1.seconds, 1, ThrottleMode.shaping)
      .map(each => s"$id-$each")
      .wireTap(each => logger.info(s"Client: $id sending: $each to exchange: $exchangeName"))
      .map(message => WriteMessage(ByteString(message)))
      .via(restartFlow)
      .runWith(Sink.ignore)

    done.onComplete {
      case Success(_) => logger.info("Done sending to exchange")
      case Failure(exception) => logger.info(s"Exception during sending to exchange:", exception)
    }
  }

  private def receiveFromExchange(id: Int, connectionProvider: AmqpCachedConnectionProvider, exchangeName: String, exchangeDeclaration: ExchangeDeclaration) = {
    logger.info(s"Starting receiveFromExchange: $exchangeName...")

    val fanoutSize = 4

    // Add the index of the source to all incoming messages, to distinguish the sending source
    val mergedSources = (0 until fanoutSize).foldLeft(Source.empty[(Int, String)]) {
      case (source, fanoutBranch) =>
        source.merge(
          AmqpSource
            .atMostOnceSource(
              TemporaryQueueSourceSettings(
                connectionProvider,
                exchangeName
              ).withDeclaration(exchangeDeclaration),
              bufferSize = 1
            )
            .wireTap(msg => logger.debug(s"Route to branch: $fanoutBranch payload: ${msg.bytes.utf8String}"))
            .map(msg => (fanoutBranch, msg.bytes.utf8String))
        )
    }

    val completion: Promise[Done] = Promise[Done]()
    mergedSources
      .viaMat(KillSwitches.single)(Keep.right)
      .to(Sink.fold(Set.empty[Int]) {
        case (seen, (branch, element)) =>
          if (seen.size == fanoutSize) completion.trySuccess(Done)
          logger.info(s"Client: $id-$branch received msg: $element from exchange: $exchangeName")
          seen + branch
      })
      .run()
  }

  // Login with guest/guest
  def browserClient() = {
    val os = System.getProperty("os.name").toLowerCase
    val url = rabbitMQContainer.getHttpUrl
    if (os == "mac os x") Process(s"open $url").!
    else if (os.startsWith("windows")) Seq("cmd", "/c", s"start $url").!
  }

  browserClient()
}