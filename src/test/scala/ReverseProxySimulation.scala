
import io.gatling.core.Predef.*
import io.gatling.http.Predef.*

import scala.concurrent.duration.*

/**
  * Start [[akkahttp.ReverseProxy]]
  * Run this simulation from cmd shell:
  * sbt 'Gatling/testOnly ReverseProxySimulation'
  * or from sbt shell:
  * Gatling/testOnly ReverseProxySimulation
  */
class ReverseProxySimulation extends Simulation {
  val baseUrl = "http://127.0.0.1:8080"

  val httpProtocol = http
    .baseUrl(baseUrl)
    .acceptHeader("application/json")
    .userAgentHeader("Gatling")

  val scn = scenario("GatlingLocalClient")
    .exec(session => session.set("correlationId", 1))
    .repeat(10) {
      exec(
        http("Local Mode Request")
          .get("/")
          .header("Host", "local")
          .header("X-Correlation-ID", session => s"load-${session.userId}-${session("correlationId").as[Int]}")
          .check(status.is(200))
          .check(status.saveAs("responseStatus"))
          .check(header("X-Correlation-ID").saveAs("responseCorrelationId"))
      )
        .exec(session => {
          println(s"Got: ${session.status} response with HTTP status: ${session("responseStatus").as[String]} for id: ${session("responseCorrelationId").as[String]}")
          session
        })
        .exec(session => session.set("correlationId", session("correlationId").as[Int] + 1))
    }

  // Adjust to scale load
  val loadFactorMorning = 0.01
  val loadFactorMidday = 0.02
  val loadFactorEvening = 0.03

  val morningPeak = scenario("Morning Peak")
    .exec(scn)
    .inject(
      nothingFor(5.seconds), // initial quiet period
      rampUsers((20 * loadFactorMorning).toInt).during(10.seconds), // ramp up
      constantUsersPerSec(50 * loadFactorMorning).during(20.seconds), // peak load
      rampUsersPerSec(50 * loadFactorMorning).to(10 * loadFactorMorning).during(10.seconds), // ramp down
      constantUsersPerSec(10 * loadFactorMorning).during(10.seconds), // tail off
      nothingFor(30.seconds) // cool down period
    )

  val middayPeak = scenario("Midday Peak")
    .exec(scn)
    .inject(
      nothingFor(5.seconds),
      rampUsers((20 * loadFactorMidday).toInt).during(10.seconds),
      constantUsersPerSec(50 * loadFactorMidday).during(20.seconds),
      rampUsersPerSec(50 * loadFactorMidday).to(10 * loadFactorMidday).during(10.seconds),
      constantUsersPerSec(10 * loadFactorMidday).during(10.seconds),
      nothingFor(30.seconds)
    )

  val eveningPeak = scenario("Evening Peak")
    .exec(scn)
    .inject(
      nothingFor(5.seconds),
      rampUsers((20 * loadFactorEvening).toInt).during(10.seconds),
      constantUsersPerSec(50 * loadFactorEvening).during(20.seconds),
      rampUsersPerSec(50 * loadFactorEvening).to(10 * loadFactorEvening).during(10.seconds),
      constantUsersPerSec(10 * loadFactorEvening).during(10.seconds),
      nothingFor(30.seconds)
    )

  setUp(
    morningPeak.andThen(middayPeak).andThen(eveningPeak)
  ).protocols(httpProtocol)
}