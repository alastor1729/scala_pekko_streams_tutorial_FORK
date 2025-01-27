package sample.stream

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.*
import org.apache.pekko.stream.scaladsl.{Flow, Sink, Source, SourceQueueWithComplete}
import org.apache.pekko.{Done, NotUsed}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.parallel.CollectionConverters.*
import scala.concurrent.Future
import scala.concurrent.duration.*
import scala.util.{Failure, Success}

/**
  * n parallel publishing clients -> sourceQueue -> slowSink
  *
  * Doc:
  * https://doc.akka.io/docs/akka/current/stream/actor-interop.html?language=scala#source-queue
  *
  * Doc buffers:
  * https://doc.akka.io/docs/akka/current/stream/stream-rate.html#buffers-in-akka-streams
  *
  * Similar example: [[MergeHubWithDynamicSources]]
  *
  * Open issue:
  * https://github.com/akka/akka/issues/26696
  *
  * See also:
  * [[BoundedSourceQueue]] (= a sync variant of SourceQueue with OverflowStrategy.dropNew)
  *
  */
object PublishToSourceQueueFromMultipleThreads extends App {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system: ActorSystem = ActorSystem()

  import system.dispatcher

  val bufferSize = 100
  val maxConcurrentOffers = 1000
  val numberOfPublishingClients = 1000

  val slowSink: Sink[Seq[Int], NotUsed] =
    Flow[Seq[Int]]
      .delay(2.seconds, DelayOverflowStrategy.backpressure)
      .to(Sink.foreach(e => logger.info(s"Reached sink: $e")))

  val sourceQueue: SourceQueueWithComplete[Int] =
    Source
      .queue[Int](bufferSize, OverflowStrategy.backpressure, maxConcurrentOffers)
      .groupedWithin(10, 1.seconds)
      .to(slowSink)
      .run()

  val doneConsuming = sourceQueue.watchCompletion() // never completes
  signalWhen(doneConsuming, "consuming")

  simulatePublishingFromMultipleThreads()

  private def simulatePublishingFromMultipleThreads(): Unit = {
    (1 to numberOfPublishingClients).par.foreach(offerToSourceQueue)
  }

  private def offerToSourceQueue(each: Int) = {
    sourceQueue.offer(each).map {
      case QueueOfferResult.Enqueued => logger.info(s"enqueued $each")
      case QueueOfferResult.Dropped => logger.info(s"dropped $each")
      case QueueOfferResult.Failure(ex) => logger.info(s"Offer failed: $ex")
      case QueueOfferResult.QueueClosed => logger.info("Source Queue closed")
    }
  }

  private def signalWhen(done: Future[Done], operation: String): Unit = {
    done.onComplete {
      case Success(_) =>
        logger.info(s"Finished: $operation")
      case Failure(e) =>
        logger.info(s"Failure: $e About to terminate...")
        system.terminate()
    }
  }
}