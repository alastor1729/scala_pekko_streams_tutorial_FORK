package akkahttp

import actor.FaultyActor
import actor.FaultyActor.DoIt
import org.apache.pekko.actor.{ActorSystem, Props}
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import org.apache.pekko.http.scaladsl.model.StatusCodes.InternalServerError
import org.apache.pekko.http.scaladsl.model.{ContentTypes, HttpEntity, HttpResponse, StatusCodes}
import org.apache.pekko.http.scaladsl.server.*
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.util.Timeout
import org.slf4j.{Logger, LoggerFactory}
import spray.json.DefaultJsonProtocol

import java.nio.file.Paths
import scala.concurrent.Await
import scala.concurrent.duration.*
import scala.sys.process.{Process, stringSeqToProcess}
import scala.util.{Failure, Success}

/**
  * Shows some (lesser known) directives from the rich feature set:
  * https://doc.akka.io/docs/akka-http/current/routing-dsl/directives/alphabetically.html
  *
  * Also shows exception handling according to:
  * https://doc.akka.io/docs/akka-http/current/routing-dsl/exception-handling.html?_ga=2.19174588.527792075.1647612374-1144924589.1645384786#exception-handling
  *
  * No streams here
  *
  */
object SampleRoutes extends App with DefaultJsonProtocol with SprayJsonSupport {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system: ActorSystem = ActorSystem()

  import spray.json.*
  import system.dispatcher

  val faultyActor = system.actorOf(Props[FaultyActor](), "FaultyActor")

  final case class FaultyActorResponse(totalAttempts: Int)

  object FaultyActorResponse extends Serializable {
    implicit def responseFormat: RootJsonFormat[FaultyActorResponse] = jsonFormat1(FaultyActorResponse.apply)
  }

  val rejectionHandler = RejectionHandler.newBuilder()
    .handle { case ValidationRejection(msg, _) => complete(StatusCodes.InternalServerError, msg) }
    .handleNotFound(complete(StatusCodes.NotFound, "Page not found"))
    .result()

  val exceptionHandler: ExceptionHandler = ExceptionHandler {
    case _: IllegalArgumentException =>
      complete(StatusCodes.BadRequest, "Illegal argument passed")
    case e: RuntimeException =>
      complete(HttpResponse(InternalServerError, entity = e.getMessage))
  }

  val getFromBrowsableDir: Route = {
    val dirToBrowse = System.getProperty("java.io.tmpdir")
    logger.info(s"Browse dir: $dirToBrowse")

    // pathPrefix allows loading dirs and files recursively
    pathPrefix("entries") {
      getFromBrowseableDirectory(dirToBrowse)
    }
  }

  val parseFormData: Route =
    // Set loglevel to "DEBUG" in application.conf for verbose pekko-http log output
    logRequest("log post request") {
      post {
        path("post") {
          val minAge = 18
          formFields(Symbol("color"), Symbol("age").as[Int]) { (color, age) =>
            if (age > minAge) {
              logger.info(s"Age: $age is older than: $minAge")
              complete(s"The color is: $color and the age is: $age")
            } else {
              logger.error(s"Age: $age is younger than: $minAge")
              reject(ValidationRejection(s"Age: $age is too low"))
            }
          }
        }
      }
    }

  val getFromDocRoot: Route =
    get {
      val static = "src/main/resources"
      concat(
        pathSingleSlash {
          val appHtml = Paths.get(static, "SampleRoutes.html").toFile
          getFromFile(appHtml, ContentTypes.`text/html(UTF-8)`)
        },
        pathPrefix("static") {
          getFromDirectory(static)
        }
      )
    }

  val getFromFaultyActor =
    pathPrefix("faultyActor") {
      get {
        import org.apache.pekko.pattern.ask
        implicit val askTimeout: Timeout = Timeout(30.seconds)
        complete((faultyActor ? DoIt()).mapTo[FaultyActorResponse])
      }
    }

  // works with:
  // curl -X GET localhost:6002/acceptAll -H "Accept: application/json"
  // curl -X GET localhost:6002/acceptAll -H "Accept: text/csv"
  // curl -X GET localhost:6002/acceptAll -H "Accept: text/plain"
  // curl -X GET localhost:6002/acceptAll -H "Accept: text/xxx"
  val acceptAll: Route = get {
    path("acceptAll") { ctx =>
      // withAcceptAll: Remove/Ignore accept headers and always return application/json
      ctx.withAcceptAll.complete("""{ "foo": "bar" }""".parseJson)
    }
  }

  // Extract raw JSON from POST request
  // curl -X POST http://localhost:6002/jsonRaw -d '{"account":{"name":"TEST"}}'
  // https://stackoverflow.com/questions/77490507/apache-pekko-akka-http-extracted-string-body-from-request-doesnt-have-quo
  val jsonRaw: Route =
    path("jsonRaw")(
      post(
        entity(as[String]) {
          json =>
            println(s"JSON raw: $json")
            complete(StatusCodes.OK)
        }
      )
    )

  val okResponseXml: Route =
    path("okResponseXml") {
      val minValidXml = "<xml version=\"1.0\"/>"
      complete(StatusCodes.OK, HttpEntity(ContentTypes.`text/xml(UTF-8)`, minValidXml))
    }

  val handleErrors = handleRejections(rejectionHandler) & handleExceptions(exceptionHandler)

  val routes = {
    handleErrors {
      concat(getFromBrowsableDir, parseFormData, getFromDocRoot, getFromFaultyActor, acceptAll, jsonRaw, okResponseXml)
    }
  }

  val bindingFuture = Http().newServerAt("127.0.0.1", 6002).bind(routes)

  bindingFuture.onComplete {
    case Success(b) =>
      println("Server started, listening on: " + b.localAddress)
    case Failure(e) =>
      println(s"Server could not bind to... Exception message: ${e.getMessage}")
      system.terminate()
  }

  def browserClient() = {
    val os = System.getProperty("os.name").toLowerCase
    if (os == "mac os x") Process(s"open http://127.0.0.1:6002").!
    else if (os.startsWith("windows")) Seq("cmd", "/c", s"start http://127.0.0.1:6002").!
  }

  browserClient()

  sys.addShutdownHook {
    println("About to shutdown...")
    val fut = bindingFuture.map(serverBinding => serverBinding.terminate(hardDeadline = 3.seconds))
    println("Waiting for connections to terminate...")
    val onceAllConnectionsTerminated = Await.result(fut, 10.seconds)
    println("Connections terminated")
    onceAllConnectionsTerminated.flatMap { _ => system.terminate()
    }
  }
}
