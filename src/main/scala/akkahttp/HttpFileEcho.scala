package akkahttp

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import org.apache.pekko.http.scaladsl.marshalling.Marshal
import org.apache.pekko.http.scaladsl.model.*
import org.apache.pekko.http.scaladsl.server.Directives.{complete, logRequestResult, path, *}
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.http.scaladsl.server.directives.FileInfo
import org.apache.pekko.http.scaladsl.unmarshalling.Unmarshal
import org.apache.pekko.stream.RestartSettings
import org.apache.pekko.stream.scaladsl.{FileIO, RestartSource, Sink, Source}
import spray.json.{DefaultJsonProtocol, RootJsonFormat}

import java.io.File
import java.nio.file.Paths
import java.time.LocalTime
import scala.annotation.unchecked.uncheckedStable
import scala.collection.parallel.CollectionConverters.*
import scala.concurrent.duration.*
import scala.concurrent.{Await, Future}
import scala.sys.process.{Process, stringSeqToProcess}
import scala.util.{Failure, Success}

trait JsonProtocol extends DefaultJsonProtocol with SprayJsonSupport {

  @uncheckedStable
  final case class FileHandle(fileName: String, absolutePath: String, length: Long)

  object FileHandle extends ((String, String, Long) => FileHandle) {
    def apply(fileName: String, absolutePath: String, length: Long): FileHandle =
      new FileHandle(fileName, absolutePath, length)

    implicit def fileInfoFormat: RootJsonFormat[FileHandle] = jsonFormat3(FileHandle.apply)
  }
}

/**
  * This HTTP file upload/download round trip is inspired by:
  * https://github.com/clockfly/akka-http-file-server
  *
  * Upload/download files up to the configured size in application.conf
  *
  * Added:
  *  - Retry on upload/download
  *    Doc: https://blog.colinbreck.com/backoff-and-retry-error-handling-for-akka-streams
  *  - Browser client for manual upload
  *
  * To prove that the streaming works:
  *  - Replace testfile.jpg with a large file, eg 63MB.pdf
  *  - Run with limited Heap, eg with -Xms256m -Xmx256m
  *  - Monitor Heap, eg with visualvm.github.io
  *
  */
object HttpFileEcho extends App with JsonProtocol {
  implicit val system: ActorSystem = ActorSystem()

  import system.dispatcher

  val resourceFileName = "testfile.jpg"
  val (address, port) = ("127.0.0.1", 6002)
  val chuckSizeBytes = 100 * 1024 // to handle large files

  server(address, port)
  (1 to 50).par.foreach(each => roundtripClient(each, address, port))
  browserClient()

  def server(address: String, port: Int): Unit = {

    def throwRndRuntimeException(operation: String): Unit = {
      val time = LocalTime.now()
      if (time.getSecond % 2 == 0) {
        val msg = s"Server RuntimeException during $operation at: $time"
        println(msg)
        throw new RuntimeException(s"BOOM - $msg")
      }
    }

    def routes: Route = logRequestResult("fileecho") {
      path("upload") {
          formFields(Symbol("payload")) { payload =>
            println(s"Server received request with additional form data: $payload")

            def tempDestination(fileInfo: FileInfo): File = File.createTempFile(fileInfo.fileName, ".tmp.server")

            // Activate to simulate rnd server ex during upload and thus provoke retry on client
            //throwRndRuntimeException("upload")

            storeUploadedFile("binary", tempDestination) {
              case (metadataFromClient: FileInfo, uploadedFile: File) =>
                println(s"Server stored uploaded tmp file with name: ${uploadedFile.getName} (Metadata from client: $metadataFromClient)")
                complete(Future(FileHandle(uploadedFile.getName, uploadedFile.getAbsolutePath, uploadedFile.length())))
            }
          }
      } ~
        path("download") {
          get {
            entity(as[FileHandle]) { fileHandle =>
              println(s"Server received download request for: ${fileHandle.fileName}")

              // Activate to simulate rnd server ex during download and thus provoke retry on client
              //throwRndRuntimeException("download")

              getFromFile(new File(fileHandle.absolutePath), MediaTypes.`application/octet-stream`)
            }
          }
        } ~
        get {
          val static = "src/main/resources"
          concat(
            pathSingleSlash {
              val appHtml = Paths.get(static, "fileupload.html").toFile
              getFromFile(appHtml, ContentTypes.`text/html(UTF-8)`)
            }
          )
        }
    }

    val bindingFuture = Http().newServerAt(address, port).bindFlow(routes)
    bindingFuture.onComplete {
      case Success(b) =>
        println("Server started, listening on: " + b.localAddress)
      case Failure(e) =>
        println(s"Server could not bind to $address:$port. Exception message: ${e.getMessage}")
        system.terminate()
    }

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

  def roundtripClient(id: Int, address: String, port: Int): Unit = {
    val fileHandle = uploadClient(id, address, port)
    fileHandle.onComplete {
      case Success(each) => downloadClient(id, each, address, port)
      case Failure(exception) => println(s"Exception during upload: $exception")
    }
  }

  def uploadClient(id: Int, address: String, port: Int): Future[HttpFileEcho.FileHandle] = {

    def createEntityFrom(file: File): Future[RequestEntity] = {
      require(file.exists())

      val fileSource = FileIO.fromPath(file.toPath, chuckSizeBytes)
      val formData = Multipart.FormData(Multipart.FormData.BodyPart(
        "binary",
        HttpEntity(MediaTypes.`application/octet-stream`, file.length(), fileSource),
        // Set the Content-Disposition header
        // see: https://www.w3.org/Protocols/HTTP/Issues/content-disposition.txt
        Map("filename" -> file.getName)),
        // Pass additional (json) payload in a form field
        Multipart.FormData.BodyPart.Strict("payload", s"{\"payload\": \"sent from Scala client with id: $id\"}", Map.empty)
      )

      Marshal(formData).to[RequestEntity]
    }

    def getResponse(request: HttpRequest): Future[FileHandle] = {
      val restartSettings = RestartSettings(1.second, 60.seconds, 0.2).withMaxRestarts(10, 1.minute)
      RestartSource.withBackoff(restartSettings) { () =>
        val responseFuture = Http().singleRequest(request)

        Source.future(responseFuture)
          .mapAsync(parallelism = 1) {
            case HttpResponse(StatusCodes.OK, _, entity, _) =>
              Unmarshal(entity).to[FileHandle]
            case HttpResponse(StatusCodes.InternalServerError, _, _, _) =>
              throw new RuntimeException(s"Response has status code: ${StatusCodes.InternalServerError}")
            case HttpResponse(statusCode, _, _, _) =>
              throw new RuntimeException(s"Response has status code: $statusCode")
          }
      }.runWith(Sink.head)
    }

    def upload(file: File): Future[FileHandle] = {

      def delayRequestSoTheServerIsNotHammered(): Unit = {
        val (start, end) = (1000, 5000)
        val rnd = new scala.util.Random
        val sleepTime = start + rnd.nextInt((end - start) + 1)
        Thread.sleep(sleepTime.toLong)
      }

      delayRequestSoTheServerIsNotHammered()

      val target = Uri(s"http://$address:$port").withPath(org.apache.pekko.http.scaladsl.model.Uri.Path("/upload"))

      val result: Future[FileHandle] =
        for {
          request <- createEntityFrom(file).map(entity => HttpRequest(HttpMethods.POST, uri = target, entity = entity))
          response <- getResponse(request)
          responseBodyAsString <- Unmarshal(response).to[FileHandle]
        } yield responseBodyAsString

      result.onComplete(res => println(s"UploadClient with id: $id received result: $res"))
      result
    }

    upload(Paths.get(s"src/main/resources/$resourceFileName").toFile)
  }

  def downloadClient(id: Int, remoteFile: FileHandle, address: String, port: Int): Future[File] = {
    val target = Uri(s"http://$address:$port").withPath(org.apache.pekko.http.scaladsl.model.Uri.Path("/download"))

    def getResponseDownload(request: HttpRequest): Future[HttpResponse] = {
      val restartSettings = RestartSettings(1.second, 60.seconds, 0.2).withMaxRestarts(10, 1.minute)
      RestartSource.withBackoff(restartSettings) { () =>
        val responseFuture = Http().singleRequest(request)

        Source.future(responseFuture)
          .mapAsync(parallelism = 1) {
            case resp@HttpResponse(StatusCodes.OK, _, _, _) => Future(resp)
            case HttpResponse(StatusCodes.InternalServerError, _, _, _) =>
              throw new RuntimeException(s"Response has status code: ${StatusCodes.InternalServerError}")
            case HttpResponse(statusCode, _, _, _) =>
              throw new RuntimeException(s"Response has status code: $statusCode")
          }
      }.runWith(Sink.head)
    }


    def saveResponseToFile(response: HttpResponse, localFile: File) = {
      response.entity.dataBytes
        .runWith(FileIO.toPath(Paths.get(localFile.getAbsolutePath)))
    }

    def download(remoteFileHandle: FileHandle, localFile: File): Unit = {

      val result = for {
        reqEntity <- Marshal(remoteFileHandle).to[RequestEntity]
        response <- getResponseDownload(HttpRequest(HttpMethods.GET, uri = target, entity = reqEntity))
        downloaded <- saveResponseToFile(response, localFile)
      } yield downloaded

      val ioresult = Await.result(result, 180.seconds)
      println(s"DownloadClient with id: $id finished downloading: ${ioresult.count} bytes to file: ${localFile.getAbsolutePath}")
    }

    val localFile = File.createTempFile("downloadLocal", ".tmp.client")
    download(remoteFile, localFile)
    Future(localFile)
  }

  def browserClient() = {
    val os = System.getProperty("os.name").toLowerCase
    if (os == "mac os x") Process(s"open http://$address:$port").!
    else if (os.startsWith("windows")) Seq("cmd", "/c", s"start http://$address:$port").!
  }
}