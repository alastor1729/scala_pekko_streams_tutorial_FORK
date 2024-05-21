package tools

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.{FileIO, Flow, Source}
import org.apache.pekko.stream.{IOResult, ThrottleMode}
import org.apache.pekko.util.ByteString
import org.slf4j.{Logger, LoggerFactory}

import java.nio.file.Paths
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}

/**
  * Translate all blocks of an English .srt file to a target lang using OpenAI API
  *
  * Workflow:
  *  - Load all blocks from the .srt source file with [[SrtParser]]
  *  - Group blocks to scenes (= all blocks within a session window), depending on `maxGapSeconds`
  *  - Translate all blocks of a scene in one prompt (one line per block) via the openAI API
  *  - Continuously write translated blocks to target file
  *
  * Works with these OpenAI API endpoints:
  *  - Default:  /chat/completions (gpt-3.5-turbo)          https://platform.openai.com/docs/guides/chat/chat-vs-completions
  *  - Fallback: /completions      (gpt-3.5-turbo-instruct) https://platform.openai.com/docs/api-reference/completions/create
  *
  * Usage:
  *  - Wire .srt source file
  *  - Add API_KEY in [[OpenAICompletions]] and run this class
  *  - Scan log for WARN log messages and improve corresponding blocks in target file manually
  *  - Note that the numerical block headers in the .srt files are not interpreted, only timestamps matter
  *
  * Similar to: [[sample.stream.SessionWindow]]
  */
object SubtitleTranslator extends App {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system: ActorSystem = ActorSystem()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  val sourceFilePath = "src/main/resources/EN_challenges.srt"
  private val targetFilePath = "DE_challenges.srt"
  private val targetLanguage = "German"

  private val defaultModel = "gpt-4o"
  private val fallbackModel = "gpt-4-turbo"

  private val maxGapSeconds = 1 // gap time between two scenes (= session windows)
  private val endLineTag = "\n"
  private val maxCharPerTranslatedLine = 40 // recommendation
  private val conversationPrefix = "-"

  private var totalTokensUsed = 0

  // Sync to ensure that all blocks are readable before translation starts
  val parseResult = SrtParser(sourceFilePath).runSync()
  logger.info("Number of subtitleBlocks to translate: {}", parseResult.length)

  val source = Source(parseResult)

  val workflow = Flow[SubtitleBlock]
    .via(groupByScene(maxGapSeconds))
    .map(translateScene)

  val fileSink = FileIO.toPath(Paths.get(targetFilePath))

  val processingSink = Flow[SubtitleBlock]
    .zipWithIndex
    .map { case (block: SubtitleBlock, blockCounter: Long) =>
      ByteString(block.formatOutBlock(blockCounter + 1))
    }
    .toMat(fileSink)((_, bytesWritten) => bytesWritten)

  val done = source
    // https://platform.openai.com/docs/guides/rate-limits/overview
    .throttle(25, 60.seconds, 25, ThrottleMode.shaping)
    .via(workflow)
    .mapConcat(identity) // flatten
    .runWith(processingSink)

  terminateWhen(done)


  // Partition to session windows
  private def groupByScene(maxGap: Int) = {
    Flow[SubtitleBlock].statefulMap(() => List.empty[SubtitleBlock])(
      (stateList, nextElem) => {
        val newStateList = stateList :+ nextElem
        val lastElem = if (stateList.isEmpty) nextElem else stateList.reverse.head
        val calcGap = nextElem.start - lastElem.end
        if (calcGap < maxGap * 1000) {
          // (list for next iteration, list of output elements)
          (newStateList, Nil)
        }
        else {
          // (list for next iteration, list of output elements)
          (List(nextElem), stateList)
        }
      },
      // Cleanup function, we return the last stateList
      stateList => Some(stateList))
      .filterNot(scene => scene.isEmpty)
  }

  private def translateScene(sceneOrig: List[SubtitleBlock]) = {
    logger.info(s"About to translate scene with: ${sceneOrig.size} original blocks")

    val allLines = sceneOrig.foldLeft("")((acc, block) => acc + block.allLinesEnd)
    val toTranslate = generateTranslationPrompt(allLines)
    logger.info(s"Translation prompt: $toTranslate")

    val translatedCheap = new OpenAICompletions().runChatCompletions(defaultModel, toTranslate)
    val translated = translatedCheap match {
      case translatedCheap if !isTranslationPlausible(translatedCheap.getLeft, sceneOrig.size) =>
        logger.info(s"Translation with: $defaultModel is not plausible, lines do not match. Fallback to: $fallbackModel")
        new OpenAICompletions().runChatCompletions(fallbackModel, toTranslate)
      case _ => translatedCheap
    }

    val newTokens = translated.getRight
    totalTokensUsed = totalTokensUsed + newTokens

    val rawResponseText = translated.getLeft
    logger.debug("Response text: {}", rawResponseText)
    val seed: Vector[SubtitleBlock] = Vector.empty

    val sceneTranslated: Vector[SubtitleBlock] =
      rawResponseText
        .split(endLineTag)
        .filterNot(each => each.isEmpty)
        .zipWithIndex
        .foldLeft(seed) { (acc: Vector[SubtitleBlock], rawResponseTextSplit: (String, Int)) =>
          val massagedResult = massageResultText(rawResponseTextSplit._1)
          val origBlock =
            if (sceneOrig.isDefinedAt(rawResponseTextSplit._2)) {
              sceneOrig(rawResponseTextSplit._2)
            } else {
              // Root cause: No plausible translation provided by openAI, eg due to added lines at beginning or at end of response
              logger.warn(s"This should not happen: sceneOrig has size: ${sceneOrig.size} but access to element: ${rawResponseTextSplit._2} requested. Fallback to last original block")
              sceneOrig.last
            }
          val translatedBlock = origBlock.copy(lines = massagedResult)
          logger.info(s"Translated block to: ${translatedBlock.allLines}")
          acc.appended(translatedBlock)
        }
    logger.info(s"Finished translation of scene with: ${sceneTranslated.size} blocks")
    sceneTranslated
  }

  private def isTranslationPlausible(rawResponseText: String, originalSize: Int) = {
    val resultSize = rawResponseText
      .split(endLineTag)
      .filterNot(each => each.isEmpty)
      .length

    resultSize == originalSize
  }

  private def generateTranslationPrompt(text: String) = {
    s"""
       |Translate the text lines below from English to $targetLanguage.
       |
       |Desired format:
       |<line separated list of translated text lines, honor all line breaks>
       |
       |Text lines:
       |$text
       |
       |""".stripMargin
  }

  private def generateShortenPrompt(text: String) = {
    s"""
       |Rewrite to ${maxCharPerTranslatedLine * 2 - 10} characters at most:
       |$text
       |
       |""".stripMargin
  }

  private def massageResultText(text: String) = {
    val textCleaned = clean(text)
    // Two people conversation in one block
    if (textCleaned.startsWith(conversationPrefix)) {
      textCleaned.split(conversationPrefix).map(line => conversationPrefix + line).toList.tail
    }
    else if (textCleaned.length > maxCharPerTranslatedLine * 2 + 10) {
      logger.warn(s"Translated block text is too long (${textCleaned.length} chars). Try to shorten via API call. Check result manually")
      val toShorten = generateShortenPrompt(textCleaned)
      logger.info(s"Shorten prompt: $toShorten")
      val responseShort = new OpenAICompletions().runChatCompletions(defaultModel, toShorten)
      splitSentence(clean(responseShort.getLeft))
    }
    else splitSentence(textCleaned)
  }

  private def clean(text: String) = {
    val filtered = text.filter(_ >= ' ')
    if (filtered.startsWith("\"")) filtered.substring(1, filtered.length() - 1)
    else filtered
  }

  private def splitSentence(text: String) = {
    if (text.length > maxCharPerTranslatedLine && text.contains(",")) {
      val indexFirstComma = text.indexOf(",")
      val offset = 15
      if (indexFirstComma > offset && indexFirstComma < text.length - offset)
        List(text.substring(0, indexFirstComma + 1), text.substring(indexFirstComma + 1, text.length))
      else splitSentenceHonorWords(text)
    }
    else if (text.length > maxCharPerTranslatedLine) {
      splitSentenceHonorWords(text)
    } else {
      List(text)
    }
  }

  private def splitSentenceHonorWords(sentence: String) = {
    val words = sentence.split(" ")
    val mid = words.length / 2
    val firstHalf = words.slice(0, mid).mkString(" ")
    val secondHalf = words.slice(mid, words.length).mkString(" ")
    List(firstHalf, secondHalf)
  }

  def terminateWhen(done: Future[IOResult]): Unit = {
    done.onComplete {
      case Success(_) =>
        logger.info(s"Flow Success. Finished writing to target file: $targetFilePath. Around $totalTokensUsed tokens used. About to terminate...")
        system.terminate()
      case Failure(e) =>
        logger.info(s"Flow Failure: $e. Partial translations are in target file: $targetFilePath About to terminate...")
        system.terminate()
    }
  }
}
