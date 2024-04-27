package sample.stream_actor.typed

import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.AskPattern.*
import org.apache.pekko.stream.ThrottleMode
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.apache.pekko.util.Timeout
import sample.stream_actor.typed.CustomCache.{AddDevices, CacheRequests, CacheResponses, CachedDevices}

import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContextExecutor, Future}

/** Use typed actor [[CustomCache]] to show shared state:
  *  - Request-Response with ask from outside (= a stream)
  *  - Request with tell from outside (= a stream)
  *
  * Inspired by:
  *  - https://blog.colinbreck.com/rethinking-streaming-workloads-with-akka-streams-part-iii
  *
  * Doc ask:
  *  - https://doc.akka.io/docs/akka/current/typed/interaction-patterns.html#request-response-with-ask-from-outside-an-actor
  */
object CustomCacheRunner extends App {
  // the system is also the top level actor ref
  implicit val cache: ActorSystem[CacheRequests] = ActorSystem[CacheRequests](CustomCache.empty, "CustomCache")
  implicit val ec: ExecutionContextExecutor = cache.executionContext
  implicit val timeout: Timeout = 5.seconds

  //Request-Response with ask
  Source
    .tick(initialDelay = 0.seconds, interval = 2.seconds, tick = ())
    .mapAsync(parallelism = 1)(_ => cache.ref.ask(ref => CustomCache.Get("42", ref)))
    .map((each: CacheResponses) =>
      each match {
        case cachedDevices: CachedDevices => cache.log.info(s"Current amount of cached devices: ${cachedDevices.devices.size}")
        case _ => cache.log.info("No devices")
      })
    .recover {
      case ex => cache.log.error("Failed to read cached devices: ", ex)
    }
    .runWith(Sink.ignore)

  //Request with tell
  Source(LazyList.continually(java.util.UUID.randomUUID.toString).take(100))
    .throttle(10, 1.second, 10, ThrottleMode.shaping)
    .mapAsync(parallelism = 10)(each => Future(cache ! AddDevices(List(DeviceId(each)))))
    .runWith(Sink.ignore)
}
