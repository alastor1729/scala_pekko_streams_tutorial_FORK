package alpakka.sse_to_elasticsearch

import io.circe.*
import io.circe.generic.auto.*
import io.circe.parser.*
import opennlp.tools.namefind.{NameFinderME, TokenNameFinderModel}
import opennlp.tools.tokenize.{TokenizerME, TokenizerModel}
import opennlp.tools.util.Span
import org.apache.commons.text.StringEscapeUtils
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.HttpRequest
import org.apache.pekko.http.scaladsl.model.sse.ServerSentEvent
import org.apache.pekko.http.scaladsl.unmarshalling.Unmarshal
import org.apache.pekko.stream.connectors.elasticsearch.*
import org.apache.pekko.stream.connectors.elasticsearch.WriteMessage.createIndexMessage
import org.apache.pekko.stream.connectors.elasticsearch.scaladsl.{ElasticsearchSink, ElasticsearchSource}
import org.apache.pekko.stream.scaladsl.{Flow, RestartSource, Sink, Source}
import org.apache.pekko.stream.{ActorAttributes, RestartSettings, Supervision}
import org.opensearch.testcontainers.OpensearchContainer
import org.slf4j.{Logger, LoggerFactory}
import org.testcontainers.utility.DockerImageName
import spray.json.DefaultJsonProtocol.*
import spray.json.RootJsonFormat

import java.io.FileInputStream
import java.net.URLEncoder
import java.nio.file.Paths
import java.time.{Instant, ZoneId}
import scala.concurrent.Future
import scala.concurrent.duration.*
import scala.sys.process.{Process, stringSeqToProcess}
import scala.util.control.NonFatal

/**
  * Consume Wikipedia edits via SSE (like in [[alpakka.sse.SSEClientWikipediaEdits]]),
  * fetch the abstract from Wikipedia API,
  * do NER processing for persons in EN
  * and write the results to either:
  *  - Elasticsearch version 7.x server
  *  - Opensearch version 2.x server
  *
  * Remarks:
  *  - We still need spray.json because of the elasticsearch pekko connectors
  *
  * Doc:
  * https://pekko.apache.org/docs/pekko-connectors/current/elasticsearch.html
  * https://www.testcontainers.org/modules/elasticsearch
  * https://pekko.apache.org/docs/pekko-connectors/current/opensearch.html
  * https://github.com/opensearch-project/opensearch-testcontainers
  */
object SSEtoElasticsearch extends App {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system: ActorSystem = ActorSystem()

  import system.dispatcher

  private val decider: Supervision.Decider = {
    case NonFatal(e) =>
      logger.warn(s"Stream failed with: $e, going to restart")
      Supervision.Restart
  }

  // 2.x model from https://opennlp.apache.org/models.html
  private val tokenModel = new TokenizerModel(new FileInputStream(Paths.get("src/main/resources/opennlp-en-ud-ewt-tokens-1.2-2.5.0.bin").toFile))
  // 1.5 model from https://opennlp.sourceforge.net/models-1.5
  private val personModel = new TokenNameFinderModel(new FileInputStream(Paths.get("src/main/resources/en-ner-person.bin").toFile))

  case class Change(timestamp: Long, title: String, serverName: String, user: String, cmdType: String, isBot: Boolean, isNamedBot: Boolean, lengthNew: Int = 0, lengthOld: Int = 0)

  object Change extends ((Long, String, String, String, String, Boolean, Boolean, Int, Int) => Change) {
    def apply(timestamp: Long, title: String, serverName: String, user: String, cmdType: String, isBot: Boolean, isNamedBot: Boolean, lengthNew: Int = 0, lengthOld: Int = 0): Change =
      new Change(timestamp, title, serverName, user, cmdType, isBot, isNamedBot, lengthNew, lengthOld)

    implicit def formatChange: RootJsonFormat[Change] = jsonFormat9(Change.apply)
  }

  // Helps to carry the data through the stages, although this violates functional principles
  case class Ctx(change: Change, personsFound: List[String] = List.empty, content: String = "")

  private object Ctx extends ((Change, List[String], String) => Ctx) {
    def apply(change: Change, personsFound: List[String] = List.empty, content: String = ""): Ctx =
      new Ctx(change, personsFound, content)

    implicit def formatCtx: RootJsonFormat[Ctx] = jsonFormat3(Ctx.apply)
  }

  //  private val dockerImageName = DockerImageName
  //    .parse("docker.elastic.co/elasticsearch/elasticsearch-oss")
  //    .withTag("7.10.2")
  //  private val elasticsearchContainer = new ElasticsearchContainer(dockerImageName)
  //  elasticsearchContainer.start()
  private val dockerImageNameOS = DockerImageName
    .parse("opensearchproject/opensearch")
    .withTag("2.18.0")
  private val searchContainer = new OpensearchContainer(dockerImageNameOS)
  searchContainer.start()

  val address = searchContainer.getHttpHostAddress
  //val connectionSettings = ElasticsearchConnectionSettings(s"http://$address")
  val connectionSettings = OpensearchConnectionSettings(s"$address")
    .withCredentials("user", "password")

  // This index will be created in Elasticsearch on the fly
  private val indexName = "wikipediaedits"
  //private val searchParams = ElasticsearchParams.V7(indexName)
  private val searchParams = OpensearchParams.V1(indexName)
  private val matchAllQuery = """{"match_all": {}}"""

  private val sourceSettings = ElasticsearchSourceSettings(connectionSettings).withApiVersion(ApiVersion.V7)

  // ElasticsearchSource reads are "scroll requests". Allows to fetch the entire collection of documents
  private val elasticsearchSourceTyped = ElasticsearchSource
    .typed[Ctx](
      searchParams,
      query = matchAllQuery,
      settings = sourceSettings
    )
  private val elasticsearchSourceRaw = ElasticsearchSource
    .create(
      searchParams,
      query = matchAllQuery,
      settings = sourceSettings
    )

  private val sinkSettings =
    ElasticsearchWriteSettings(connectionSettings)
      .withBufferSize(10)
      .withVersionType("internal")
      .withRetryLogic(RetryAtFixedRate(maxRetries = 5, retryInterval = 1.second))
      .withApiVersion(ApiVersion.V7)
  private val elasticsearchSink =
    ElasticsearchSink.create[Ctx](
      searchParams,
      settings = sinkSettings
    )


  import org.apache.pekko.http.scaladsl.unmarshalling.sse.EventStreamUnmarshalling.*

  val restartSettings = RestartSettings(1.second, 10.seconds, 0.2).withMaxRestarts(10, 1.minute)
  val restartSource = RestartSource.withBackoff(restartSettings) { () =>
    Source.futureSource {
      Http()
        .singleRequest(HttpRequest(
          uri = "https://stream.wikimedia.org/v2/stream/recentchange"
        ))
        .flatMap(Unmarshal(_).to[Source[ServerSentEvent, NotUsed]])
    }.withAttributes(ActorAttributes.supervisionStrategy(decider))
  }

  val parserFlow: Flow[ServerSentEvent, Change, NotUsed] = Flow[ServerSentEvent].map {
    serverSentEvent => {

      def isNamedBot(bot: Boolean, user: String): Boolean = {
        if (bot) user.toLowerCase().contains("bot") else false
      }

      val cursor = parse(serverSentEvent.data).getOrElse(Json.Null).hcursor

      val titleAsID = cursor.get[String]("title").toOption.getOrElse("")
      val timestamp: Long = cursor.get[Long]("timestamp").toOption.getOrElse(0)
      val serverName = cursor.get[String]("server_name").toOption.getOrElse("")
      val user = cursor.get[String]("user").toOption.getOrElse("")
      val cmdType = cursor.get[String]("type").toOption.getOrElse("")
      val bot = cursor.get[Boolean]("bot").toOption.getOrElse(false)

      if (cmdType == "new" || cmdType == "edit") {
        val length = cursor.downField("length")
        val lengthNew = length.get[Int]("new").toOption.getOrElse(0)
        val lengthOld = length.get[Int]("old").toOption.getOrElse(0)
        Change(timestamp, titleAsID, serverName, user, cmdType, isBot = bot, isNamedBot = isNamedBot(bot, user), lengthNew, lengthOld)
      } else {
        Change(timestamp, titleAsID, serverName, user, cmdType, isBot = bot, isNamedBot = isNamedBot(bot, user))
      }
    }
  }

  private def fetchContent(ctx: Ctx): Future[Ctx] = {
    logger.info(s"About to read `extract` from Wikipedia entry with title: ${ctx.change.title}")
    val encodedTitle = URLEncoder.encode(ctx.change.title, "UTF-8")

    val requestURL = s"https://en.wikipedia.org/w/api.php?format=json&action=query&prop=extracts&exlimit=max&explaintext&exintro&titles=$encodedTitle"
    Http().singleRequest(HttpRequest(uri = requestURL))
      // Consume the streamed response entity
      // Doc: https://doc.akka.io/docs/akka-http/current/client-side/request-level.html
      .flatMap(_.entity.toStrict(2.seconds))
      .map(_.data.utf8String.split("\"extract\":").reverse.head)
      .map(content => ctx.copy(content = content))
  }


  private def findPersonsLocalNER(ctx: Ctx): Future[Ctx] = {
    logger.info(s"LocalNER: About to find person names in: ${ctx.change.title}")
    val content = ctx.content

    // We need a new instance, because TokenizerME is not thread safe
    // Doc: https://opennlp.apache.org/docs/2.0.0/manual/opennlp.html
    // Chapter: Name Finder API
    val tokenizer = new TokenizerME(tokenModel)
    val tokens = tokenizer.tokenize(content)

    val personNameFinderME = new NameFinderME(personModel)
    val spans = personNameFinderME.find(tokens)
    val personsFound = Span.spansToStrings(NameFinderME.dropOverlappingSpans(spans), tokens).toList.distinct
    personNameFinderME.clearAdaptiveData()

    if (personsFound.isEmpty) {
      Future(ctx)
    } else {
      val personsFoundCleaned = personsFound.map(each => StringEscapeUtils.unescapeJava(each))
      logger.info(s"FOUND persons: $personsFoundCleaned from content: $content")
      Future(ctx.copy(personsFound = personsFoundCleaned))
    }
  }

  def findPersonsRemoteGpt3NER(ctx: Ctx): Future[Ctx] = {
    logger.info(s"GPT-3 NER: About to find person names in: ${ctx.change.title}")
    val content = ctx.content
    val resultRaw = new NerRequestOpenAI().run(content)

    case class Choice(text: String, score: Double)

    val cursor = parse(resultRaw).getOrElse(Json.Null).hcursor
    val choices = cursor.downField("choices").as[Seq[Choice]].toOption.getOrElse(List.empty)
    val personsFound = choices.head.text.split("\n").filter(_.nonEmpty).toList
    if (personsFound.isEmpty) {
      Future(ctx)
    } else {
      val personsFoundCleaned = personsFound.map(each => StringEscapeUtils.unescapeJava(each))
      logger.info(s"FOUND persons: $personsFoundCleaned from content: $content")
      Future(ctx.copy(personsFound = personsFoundCleaned))
    }
  }

  private val nerProcessingFlow: Flow[Change, Ctx, NotUsed] = Flow[Change]
    .filter(change => !change.isBot)
    .map(change => Ctx(change))
    .mapAsync(3)(ctx => fetchContent(ctx))
    .mapAsync(3)(ctx => findPersonsLocalNER(ctx))
    //TODO Activate, when results are better
    //.mapAsync(3)(ctx => findPersonsRemoteGpt3NER(ctx))
    .filter(ctx => ctx.personsFound.nonEmpty)

  logger.info(s"Elasticsearch/Opensearch container listening on: ${searchContainer.getHttpHostAddress}")
  logger.info("About to start processing flow...")

  restartSource
    .via(parserFlow)
    .via(nerProcessingFlow)
    .map(ctx => createIndexMessage(dateTimeFormatted(ctx.change.timestamp), ctx))
    .wireTap(each => logger.info(s"Add to index: $each"))
    .withAttributes(ActorAttributes.supervisionStrategy(decider))
    .runWith(elasticsearchSink)


  // Wait for the index to populate
  Thread.sleep(10.seconds.toMillis)
  browserClient()

  Source.tick(1.seconds, 10.seconds, ())
    .map(_ => query())
    .runWith(Sink.ignore)

  private def browserClient() = {
    val os = System.getProperty("os.name").toLowerCase
    val url = s"http://localhost:${searchContainer.getMappedPort(9200)}/$indexName/_search?q=personsFound:*&size=100"
    if (os == "mac os x") Process(s"open $url").!
    else if (os.startsWith("windows")) Seq("cmd", "/c", s"start $url").!
    else logger.info(s"Please open a browser at: $url")
  }

  private def dateTimeFormatted(timestamp: Long) = {
    Instant.ofEpochSecond(timestamp).atZone(ZoneId.systemDefault).toLocalDateTime.toString
  }

  // Note that the size of the collection can also be fetched via a GET request, eg
  // http://localhost:57321/wikipediaedits/_count
  private def query(): Unit = {
    logger.info(s"About to execute scrolled read queries...")
    for {
      result <- readFromElasticsearchTyped()
      resultRaw <- readFromElasticsearchRaw()
    } {
      logger.info(s"Read typed: ${result.size}. 1st element: ${result.head}")
      logger.info(s"Read raw: ${resultRaw.size}. 1st element: ${resultRaw.head}")
    }
  }

  private def readFromElasticsearchTyped() = {
    elasticsearchSourceTyped.runWith(Sink.seq)
  }

  private def readFromElasticsearchRaw() = {
    elasticsearchSourceRaw.runWith(Sink.seq)
  }
}
