package alpakka.file

import alpakka.file.uploader.DirectoryWatcher
import org.apache.commons.io.FileUtils
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEachTestData, TestData}
import org.slf4j.{Logger, LoggerFactory}

import java.nio.file.{Files, Path, Paths}
import scala.concurrent.Await
import scala.concurrent.duration.*
import scala.util.Random

/**
  * Designed as IT test on purpose to demonstrate
  * the realistic usage of [[DirectoryWatcher]]
  * Hence we:
  *  - create the dir structure and copy files before each test
  *  - clean up dir structure after each test
  *  - use a shared watcher instance for all tests
  */
final class DirectoryWatcherSpec extends AsyncWordSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEachTestData {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  var watcher: DirectoryWatcher = _
  var tmpRootDir: Path = _
  var uploadDir: Path = _
  var processedDir: Path = _

  "DirectoryWatcher" should {
    "detect_files_on_startup_in_parent_dir" in {
      watcher = DirectoryWatcher(uploadDir, processedDir)
      waitForCondition(3.seconds)(watcher.countFilesProcessed() == 2) shouldBe true
    }

    "detect_added_file_at_runtime_in_parent_dir" in {
      watcher = DirectoryWatcher(uploadDir, processedDir)
      copyTestFileToDir(uploadDir)
      waitForCondition(3.seconds)(watcher.countFilesProcessed() == 2 + 1) shouldBe true
    }

    "detect_added_files_at_runtime_in_sub_dir" in {
      watcher = DirectoryWatcher(uploadDir, processedDir)
      copyTestFileToDir(uploadDir.resolve("subdir"))
      waitForCondition(3.seconds)(watcher.countFilesProcessed() == 2 + 1) shouldBe true
    }

    "detect_added_nested_subdir_at_runtime_with_files_in_subdir" in {
      watcher = DirectoryWatcher(uploadDir, processedDir)
      val tmpDir = Files.createTempDirectory("tmp")
      val sourcePath = Paths.get("src/main/resources/testfile.jpg")
      val targetPath = tmpDir.resolve(createUniqueFileName(sourcePath.getFileName))
      val targetPath2 = tmpDir.resolve(createUniqueFileName(sourcePath.getFileName))
      Files.copy(sourcePath, targetPath)
      Files.copy(sourcePath, targetPath2)
      val targetDir = Files.createDirectories(uploadDir.resolve("subdir").resolve("nestedDirWithFiles"))
      FileUtils.copyDirectory(tmpDir.toFile, targetDir.toFile)
      waitForCondition(3.seconds)(watcher.countFilesProcessed() == 2 + 2) shouldBe true
    }

    "handle_large_number_of_files_in_parent_dir" in {
      (1 to 1000).foreach(_ => copyTestFileToDir(uploadDir))
      watcher = DirectoryWatcher(uploadDir, processedDir)
      waitForCondition(10.seconds)(watcher.countFilesProcessed() == 2 + 1000) shouldBe true
    }

    "handle_invalid_parent_directory_path" in {
      val invalidParentDir = Paths.get("/path/to/non-existent/directory")
      val processedDir = Files.createTempDirectory("processed")

      the[IllegalArgumentException] thrownBy {
        watcher = DirectoryWatcher(invalidParentDir, processedDir)
      } should have message s"Invalid upload directory path: $invalidParentDir"
    }
  }

  override protected def beforeEach(testData: TestData): Unit = {
    logger.info(s"Starting test: ${testData.name}")

    tmpRootDir = Files.createTempDirectory(testData.text)
    logger.info(s"Created tmp root dir: $tmpRootDir")

    uploadDir = tmpRootDir.resolve("upload")
    processedDir = tmpRootDir.resolve("processed")
    Files.createDirectories(uploadDir)
    Files.createDirectories(uploadDir.resolve("subdir"))
    Files.createDirectories(processedDir)

    // Populate dirs BEFORE startup
    copyTestFileToDir(tmpRootDir.resolve("upload"))
    copyTestFileToDir(tmpRootDir.resolve("upload/subdir"))
  }

  override protected def afterEach(testData: TestData): Unit = {
    logger.info(s"Cleaning up after test: ${testData.name}")
    if (watcher != null) Await.result(watcher.stop(), 5.seconds)
    FileUtils.deleteDirectory(tmpRootDir.toFile)
    logger.info(s"Finished test: ${testData.name}")
  }

  private def copyTestFileToDir(target: Path) = {
    val sourcePath = Paths.get("src/main/resources/testfile.jpg")
    val targetPath = target.resolve(createUniqueFileName(createUniqueFileName(sourcePath.getFileName)))
    Files.copy(sourcePath, targetPath)
  }

  private def createUniqueFileName(fileName: Path) = {
    val parts = fileName.toString.split('.').map(_.trim)
    Paths.get(s"${parts.head}${Random.nextInt()}.${parts.reverse.head}")
  }

  private def waitForCondition(maxDuration: FiniteDuration)(condition: => Boolean): Boolean = {
    val startTime = System.currentTimeMillis()
    var elapsed = 0.millis

    while (!condition && elapsed < maxDuration) {
      Thread.sleep(100)
      elapsed = (System.currentTimeMillis() - startTime).millis
    }
    logger.info("Condition reached after: {} ms", elapsed.toMillis)
    condition
  }
}
