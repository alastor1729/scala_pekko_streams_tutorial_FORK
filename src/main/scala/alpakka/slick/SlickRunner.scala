package alpakka.slick

import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.connectors.slick.scaladsl.*
import org.apache.pekko.stream.scaladsl.*
import org.slf4j.{Logger, LoggerFactory}
import slick.jdbc.{GetResult, ResultSetConcurrency, ResultSetType}

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}

/**
  * DB access via Slick, for simplicity using the 'public' schema
  * Run with integration test: [[alpakka.slick.SlickIT]]
  *
  * Doc:
  * https://doc.akka.io/docs/alpakka/current/slick.html
  * https://scala-slick.org/docs
  * https://blog.rockthejvm.com/slick
  */
//noinspection SqlNoDataSourceInspection
class SlickRunner(urlWithMappedPort: String) {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system: ActorSystem = ActorSystem()

  import system.dispatcher

  val counter = new AtomicInteger()

  // Tweak config url param with mapped port from container
  val tweakedConf: Config = ConfigFactory.empty()
    .withValue("slick-postgres.db.url", ConfigValueFactory.fromAnyRef(urlWithMappedPort))
    .withFallback(ConfigFactory.load())

  implicit val session: SlickSession = SlickSession.forConfig("slick-postgres", tweakedConf)

  import session.profile.api.*

  case class User(id: Int, name: String)

  class Users(tag: Tag) extends Table[(Int, String)](tag, Some("public"), "users") {
    def id = column[Int]("id")

    def name = column[String]("name")

    def * = (id, name)
  }

  implicit val getUserResult: AnyRef & GetResult[User] = GetResult(r => User(r.nextInt(), r.nextString()))

  def insertUser(user: User): DBIO[Int] =
    sqlu"INSERT INTO public.users VALUES(${user.id}, ${user.name})"

  def readUsersAsync(): Future[Set[User]] = {
    val selectAllUsers = sql"SELECT id, name FROM public.users".as[User]
    Slick.source(selectAllUsers).runWith(Sink.seq).map(_.toSet)
  }

  def readUsersSync(): Set[User] = {
    logger.info("About to read...")
    val result = Await.result(readUsersAsync(), 10.seconds)
    logger.info(s"Successfully read: ${result.size} users")
    result
  }

  def processUsersPaged(): Future[Seq[Int]] = {
    val typedSelectAllUsers = TableQuery[Users]
      .result
      .withStatementParameters(
        rsType = ResultSetType.ForwardOnly,
        rsConcurrency = ResultSetConcurrency.ReadOnly,
        fetchSize = 10
      )
      .transactionally

    val result = Slick.source(typedSelectAllUsers)
      .grouped(1000)
      .wireTap((group: Seq[(Int, String)]) => {
        // Simulate some processing on each group
        val sum = group.map(_._1).sum
        logger.info(s"Group PK sum: $sum")
      })

      .map(each => counter.getAndAdd(each.size))
      .runWith(Sink.seq)

    result.onComplete(res => logger.info(s"Done reading paged, yielding: ${counter.get()} elements"))
    result
  }

  // Discussion:
  // https://stackoverflow.com/questions/69674054/how-do-you-transform-a-fixedsqlaction-into-a-streamingdbio-in-slick/69682195#69682195
  def getTotal: Int = {
    val query = TableQuery[Users].length.result
    Await.result(session.db.run(query), 60.seconds)
  }

  def createTableOnSession(): Int = {
    val createTable = sqlu"""CREATE TABLE public.users(id INTEGER, name VARCHAR(50))"""
    Await.result(session.db.run(createTable), 2.seconds)
  }

  def dropTableOnSession(): Int = {
    val dropTable = sqlu"""DROP TABLE public.users"""
    Await.result(session.db.run(dropTable), 2.seconds)
  }

  def populateSync(noOfUsers: Int = 100): Unit = {
    logger.info(s"About to populate DB with: $noOfUsers users...")
    val users = (1 to noOfUsers).map(i => User(i, s"Name$i")).toSet
    val actions = users.map(insertUser)

    // Insert via standard Slick API, this may take some time
    Await.result(session.db.run(DBIO.seq(actions.toList *)), 60.seconds)
    logger.info("Done populating DB")
  }

  def terminate(): Unit = {
    system.terminate()
    logger.info("About to close session...")
    session.close()
  }
}

object SlickRunner extends App {
  def apply(urlWithMappedPort: String) = new SlickRunner(urlWithMappedPort)
}