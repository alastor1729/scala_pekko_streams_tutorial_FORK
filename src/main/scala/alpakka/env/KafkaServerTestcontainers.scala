package alpakka.env

import org.slf4j.{Logger, LoggerFactory}
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName

/**
  * Uses testcontainers.org to run the
  * latest Kafka-Version from Confluent
  * See also Kafka broker from: /docker/docker-compose.yml
  *
  * Alternative: [[KafkaServerEmbedded]]
  *
  * Doc:
  * https://www.testcontainers.org/modules/kafka
  * https://doc.akka.io/docs/alpakka-kafka/current/testing-testcontainers.html
  * https://doc.akka.io/docs/alpakka-kafka/current/testing.html
  */
class KafkaServerTestcontainers {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val kafkaVersion = "7.7.0"
  val imageName = s"confluentinc/cp-kafka:$kafkaVersion"
  val originalPort = 9093
  var mappedPort = 1111
  val kafkaContainer: KafkaContainer = new KafkaContainer(DockerImageName.parse(imageName)).
    withExposedPorts(originalPort)

  def run(): Unit = {
    kafkaContainer.start()
    mappedPort = kafkaContainer.getMappedPort(originalPort)
    logger.info(s"Running Kafka: $imageName on mapped port: $mappedPort")
  }

  def stop(): Unit = {
    kafkaContainer.stop()
  }
}

object KafkaServerTestcontainers extends App {
  val server = new KafkaServerTestcontainers()
  server.run()

  sys.ShutdownHookThread{
    println("Got control-c cmd from shell or SIGTERM, about to shutdown...")
    server.stop()
  }

  Thread.currentThread.join()

  def apply(): KafkaServerTestcontainers = new KafkaServerTestcontainers()
  def mappedPort(): Int = server.mappedPort
}