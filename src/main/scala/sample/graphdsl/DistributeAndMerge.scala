package sample.graphdsl

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.*
import org.apache.pekko.stream.scaladsl.*

import scala.concurrent.Future
import scala.util.hashing.MurmurHash3


/**
  * Inspired by, stolen from:
  * https://gist.github.com/calvinlfer/cc4ea90328834a95a89ce99aeb998a63
  *
  * Concepts:
  *  - Flow that distributes messages (according to a hashing function) across sub-flows
  *  - The idea is to have ordered processing per sub-flow but parallel processing across sub-flows
  *
  * Similar examples:
  *  - https://blog.colinbreck.com/partitioning-akka-streams-to-maximize-throughput
  *  - https://doc.akka.io/docs/akka/current/stream/stream-cookbook.html#balancing-jobs-to-a-fixed-pool-of-workers
  */
object DistributeAndMerge extends App {
  implicit val system: ActorSystem = ActorSystem()

  import system.dispatcher

  private def sampleAsyncCall(x: Int): Future[Int] = Future {
    Thread.sleep((x * 100L) % 10)
    println(s"Async call for value: $x processed by: ${Thread.currentThread().getName}")
    x
  }

  // @formatter:off
  /**
    * Example based on numBuckets = 3
    *                                          --- bucket 1 flow --- ~mapAsync(parallelism)~ ---
    *                   |------------------| /                                                  \|---------------|
    * Open inlet[A] --- | Partition Fan Out|  --- bucket 2 flow --- ~mapAsync(parallelism)~ -----| Merge Fan In  | --- Open outlet[B]
    *                   |------------------| \                                                  /|---------------|
    *                                         --- bucket 3 flow --- ~mapAsync(parallelism)~ ---
    *
    * @param numBuckets  the number of sub-flows to create
    * @param parallelism the mapAsync (ordered) parallelism per sub-flow
    * @param hash        the hashing function used to decide
    * @param fn          the mapping function to be used for mapAsync
    * @tparam A is the input stream of elements of type A
    * @tparam B is the output streams of elements of type B
    * @return a Flow of elements from type A to type B
    */
  // @formatter:on
  private def hashingDistribution[A, B](numBuckets: Int,
                                        parallelism: Int,
                                        hash: A => Int,
                                        fn: A => Future[B]): Flow[A, B, NotUsed] = {
    Flow.fromGraph(GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits.*
      val numPorts = numBuckets
      val partitioner =
        builder.add(Partition[A](outputPorts = numPorts, partitioner = a => math.abs(hash(a)) % numPorts))
      val merger = builder.add(Merge[B](inputPorts = numPorts, eagerComplete = false))

      Range(0, numPorts).foreach { eachPort =>
        partitioner.out(eachPort) ~> Flow[A].mapAsync(parallelism)(fn) ~> merger.in(eachPort)
      }

      FlowShape(partitioner.in, merger.out)
    })
  }

  Source(1 to 10)
    .via(
      hashingDistribution[Int, Int](
        numBuckets = 3,
        parallelism = 2,
        hash = element => MurmurHash3.stringHash(element.toString), //Hashing function: String => Int
        fn = sampleAsyncCall
      )
    )
    .runWith(Sink.foreach(each => println(s"Reached sink: $each")))
    .onComplete(_ => system.terminate())
}
