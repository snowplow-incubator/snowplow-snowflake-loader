/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package com.snowplowanalytics.snowplow.snowflake.processing

import cats.Id
import cats.effect.{IO, Ref}
import cats.effect.kernel.Outcome
import doobie.Transactor
import org.specs2.Specification
import org.specs2.matcher.MatchResult
import cats.effect.testing.specs2.CatsEffect
import cats.effect.testkit.TestControl

import com.snowplowanalytics.snowplow.runtime.HealthProbe
import com.snowplowanalytics.snowplow.snowflake.{Alert, AppHealth, Config, MockEnvironment, Monitoring}

import scala.concurrent.duration.DurationLong

class JdbcTransactorSpec extends Specification with CatsEffect {
  import JdbcTransactorSpec._

  def is = s2"""
  The JdbcTransactor when given a valid config should
    Provide a transactor ${healthy1(goodConfig)}
    Provide a transactor when key is encrypted ${healthy1(goodConfigWithEncryptedKey)}
    Not send any alerts ${healthy2(goodConfig)} ${healthy2(goodConfigWithEncryptedKey)}
  The JdbcTransactor when given an invalid config should
    Sleep forever instead of providing a transactor ${unhealthy1(badConfigWithEncryptedKey)} ${unhealthy1(badConfigWithMissingPassphrase)}
    Send a monitoring alert ${unhealthy2(badConfigWithEncryptedKey)} ${unhealthy2(badConfigWithMissingPassphrase)}
    Change app health from unhealthy to healthy ${unhealthy3(badConfigWithEncryptedKey)} ${unhealthy3(badConfigWithEncryptedKey)}
  """

  def healthy1(config: Config.Snowflake) = MockEnvironment.build(Nil, MockEnvironment.Mocks.default).use { c =>
    val io = for {
      monitoring <- testMonitoring
      result <- JdbcTransactor.make(config, monitoring, c.environment.appHealth)
    } yield result

    afterOneDay(io) { case Some(Outcome.Succeeded(_: Transactor[IO])) =>
      ok
    }
  }

  def healthy2(config: Config.Snowflake) = MockEnvironment.build(Nil, MockEnvironment.Mocks.default).use { c =>
    val io = for {
      monitoring <- testMonitoring
      _ <- JdbcTransactor.make(config, monitoring, c.environment.appHealth)
      alerts <- monitoring.ref.get
    } yield alerts

    afterOneDay(io) { case Some(Outcome.Succeeded(alerts)) =>
      alerts must beEmpty
    }
  }

  def unhealthy1(config: Config.Snowflake) = MockEnvironment.build(Nil, MockEnvironment.Mocks.default).use { c =>
    val io = for {
      monitoring <- testMonitoring
      _ <- JdbcTransactor.make(config, monitoring, c.environment.appHealth)
    } yield ()

    afterOneDay(io) { case None =>
      ok
    }
  }

  def unhealthy2(config: Config.Snowflake) = MockEnvironment.build(Nil, MockEnvironment.Mocks.default).use { c =>
    val io = for {
      monitoring <- testMonitoring
      _ <- JdbcTransactor.make(config, monitoring, c.environment.appHealth).start
      _ <- IO.sleep(6.hours)
      alerts <- monitoring.ref.get
    } yield alerts

    afterOneDay(io) { case Some(Outcome.Succeeded(alerts)) =>
      alerts must beLike { case Vector(Alert.FailedToParsePrivateKey(_)) =>
        ok
      }
    }
  }

  def unhealthy3(config: Config.Snowflake) = MockEnvironment.build(Nil, MockEnvironment.Mocks.default).use { c =>
    val io = for {
      monitoring <- testMonitoring
      _ <- c.environment.appHealth.setServiceHealth(AppHealth.Service.Snowflake, true)
      healthBeforeTest <- c.environment.appHealth.status()
      _ <- JdbcTransactor.make(config, monitoring, c.environment.appHealth).start
      _ <- IO.sleep(6.hours)
      healthAfterTest <- c.environment.appHealth.status()
    } yield (healthBeforeTest, healthAfterTest)

    afterOneDay(io) { case Some(Outcome.Succeeded((healthBeforeTest, healthAfterTest))) =>
      (healthBeforeTest, healthAfterTest) must beLike { case (HealthProbe.Healthy, HealthProbe.Unhealthy(_)) =>
        ok
      }
    }
  }

  def afterOneDay[A](io: IO[A])(pf: PartialFunction[Option[Outcome[Id, Throwable, A]], MatchResult[Any]]): IO[MatchResult[Any]] =
    TestControl.execute(io).flatMap { ioControl =>
      for {
        _ <- ioControl.tickFor(1.day)
        results <- ioControl.results
      } yield results must beLike(pf)
    }

}

object JdbcTransactorSpec {

  val goodUnencryptedKey = """-----BEGIN PRIVATE KEY-----
  |MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDDcm38cgshuiF2
  |IbilphDODAluv3r8paiPiKUGcwxLt6hY1kooVjvvu2dpKlvd34g3pfppneN1yJhc
  |2+Me/iJD644TyBcp9fa6/F/bfczwXxbDPZ7Lc5OUfnGo4IVuDw2HaE1AAuT/Jgnq
  |CUnx6RAF83AAvm1TtFQdRnbeFln1ril4zKQdD6w6sxu8ucbF9egTh4FpUDgNp0Hg
  |K4SdjONp4cPt9LrN/0vGh3yh6svv9HApktBh9K2Wjjaxntmd9MlKpGCUrLWXDMun
  |i16OIQ3bcOFh7aAP1SMRSHKAJiHqZIUBIJMlVJGKQCHfrMQ6CJSxO/sDJ1vC+CNd
  |pTKy5JF9AgMBAAECggEADRznTjGkl42yYweeKNb8d6aNF3YXXU4MAh1L3SPo5keb
  |LuKptQ0cFlh/dqnV4gv2Sq2DIITsVuGvf0NteI3aZK4wKRmanEEZXbBDCinljxcC
  |IvVGayYE98iH/amaqiiuYrBXxnyrOocl0SLwaB+X6J5NnG8qTJxjrFcm8H2VaYs4
  |zCe3oJO7waZF/HrBeZZkG2zfae+p/EqqD72Pf/MVh431ApseWecEg+MPpgfBRita
  |BieY6jEjQbiWXJgfS9bMZWVCLNcclwdQ7PS3yXV4uzxEbYiCcHKUwDap/2U8mena
  |rJAmNykQSbgk1SwjgDMjUQ/ctAUASJZY3DQXACYmIQKBgQDmEr8neVBxs0rsB64D
  |dzWLYzUaiuK9QVXITfF6dy8MCOwEm62yOvBVdhJR0cwUgxiPfmV/OV9NirbMtKEM
  |+/ssWHFqic4J/nDVFrEeWrrzWk/onIrDgNGWf3XktVE2DmfKdCrssryDqZC0corl
  |7w8OTMLGUybdTMp4U/35kGRNCQKBgQDZeMUhVMcRsX/YcpJuk0kzTc/v0SYL44LE
  |mFyq3qlxdDP0CY0ie1c/vLdVe+h+mv378fAuLf664UpljYb5wbNEjfYz7RoW4+Um
  |d19NSlK0ZqNZbiyK4FkfiF3OtLGjp+khz7EUAKNhnOPpPrs+KfvwsX14OzoOs0Y2
  |J7LMIVPx1QKBgQCmCHtYgkzScOAtq3Eh4SKL/8Ev8XClwYOldNJCXcZe+gVRYgOc
  |rroIApg/4ZZUazMLQtz+Tin/rI409lmPJD1kCEN47g/52FwW+zRAwptNySwHowjl
  |A468/CjZLxx3VTgDu4fKn0Y6AeGCx3KDcty7phudwh428BbhdUPAmTo4+QKBgARH
  |1kmDq69zePq/tpYqnARAgdlMmp0dS1OnVBug6mDrUqJ5FagaGWuNwWYTXE4xqtIs
  |vveJvDvdd2NsV73OzEKLMM9w2VSeA8KwEtYoolwesRRvkLzjEZ4HRyFseRqpkXMy
  |7V9ha9XeCrZqn7Dnjqf8NmYJdGkZqkYinehat5ZJAoGBAICsQjZieYf01Z1KXzCc
  |BC16iq6Mulic4JCDncPfMP/1bUtNiqm5vbRjt4rUJYt6GM78hvjQu9++TSCbeg9E
  |zs8wGxJAaWbrvWSqXHwQyj35u0UkUkf58Dqb2bBepwEAyBdiFSgzJPdR7Y6rWFQk
  |isyi39rDQSFfHqyi9WCjRzcg
  |-----END PRIVATE KEY-----""".stripMargin

  val goodEncryptedKey = """-----BEGIN ENCRYPTED PRIVATE KEY-----
  |MIIFHDBOBgkqhkiG9w0BBQ0wQTApBgkqhkiG9w0BBQwwHAQIq95GfE2zqkcCAggA
  |MAwGCCqGSIb3DQIJBQAwFAYIKoZIhvcNAwcECHuOkUb6DmoHBIIEyIO0xBYFGyFY
  |fSEnQdfKCh2UnzrkxOVjCQyKake91BNmd3VREkeu8axSTkM9GGkuY8LzD/qCBegP
  |weFEI2ij7xCKrvTiXpvGRkUhXMimh6i+bngkxWfn3oJg2zL15W34DVYZ50MzhPsf
  |XUtFXFOdG3oDku94SqtwTk6FpQPd2oQyb1ERtbTXhYOneIpDEVoJ48/sNluMOrbe
  |/pMjZy8rFBJKZgKTwKAjHqJW7dF4hfXvWqWFEkzZzO+fnJx4s9ufdk1ftbW0iO45
  |xfqzMTN4mlD9DZYktM2YGuoIaLqKphz6vSmoawzNKCJmAYBxjuPXFpoLSyozZ+NH
  |42u9Q9muaZuT3RaJ6myvP+TbmI11zupecmqGalH4xFQDWyz6y154Gl09qQMry9VO
  |Lb++2v/ZvsgkV/YL5bN3WtSKnXdCATFna16WStzw4E25cEUuDxCd24WMYyq7jqKK
  |qMWHfNG/yoTKFiCkazEf5/04DFEBZJrpiIsv8K85dy2uPggYT6q1HrJIY0cu84ub
  |lk2Ev8Xk89H/tdXbchRzjkw4WC2jwpkunUVqW4Rz58QsIfeaYZju+P8AWof07nn/
  |O361CDuM4pvE2F1oaKt6tRoQgqpnAaYkqLxi3U5VAw6uOWuHOY06Srht1aGn7lC/
  |TXtjBdeN0MYrUpNy8QXbZZ2zKedxLQyUEICa8wPeJd1+gmP3w5UJET2LNIY/Pesh
  |SqDyQD7BzAGoz+QINYL+vRmR/8q+KyfX1orsmjIP3d9bCyq9chr18hgiSfWLIm8d
  |woh2SDibjA3L8RBESukvPFifoU+n8E3AGZ9s7uuIzhkaSYHTVp2TeLRME4OqB4Cp
  |EVMSiNqIOjrtv1EviUCTUyiPF/GzYzYFlXyvH5f2YuxqXvjvbAhv7VbXJoAhKF/q
  |L4Ndsa5LxNNh6xLJKuTAeOYYlgSoAXRkRMwkhOlU73yonIeroeqqXYYjK45BCTEE
  |YPvAER37Ttg5UrHwpVPzu8ltnZKnyRff190HK1GRvXHVScwSL5h1E+YcX/bIoVoN
  |LcGFjqXhXsc1M/meJ4VnriwXQPJfQtk0ndfPiQoDrGNjHE5MqoOdTwSHT08qlkOT
  |1AERbJTc3OzYtWKKgVOcqTeVeUfubWLdD0oRjxBuTi5iynZn9RjdpZ1lzzT8zajq
  |Eh7APzc6ILLDU09MeKZp4HaiPvKEUumo0dxpgsWRKke9xcMO+KKgr06+gANHhwnC
  |4FA8II9JZIoxSUgoaAysExaA8GD4KmZg/43i8ulZNgbEUV0VyV0rN3g3IkCKfImU
  |muF9/r5e8meuj5jibvLMQxiDe5DEMiBkxqteMdXtLIMr9FUIwahhC1kQSgwF5Vqj
  |lppulhtXWMx/gRF6ERn0Wk+IBYcl2FKvvIGFsP07BbyAEFQE05trtuZ+a/N1E9Ie
  |V+SKr8V7hpAio1uHgWJf18rbXLQ5vkTvlKLduHcdgWusVo4Uv57/GKlY7cwqg3tT
  |E45BWdNvJZdat3XEc78U0jw4qEhOeVyEZy6Ak9AzLiYHAhwadenWeeOYLCz3LdMv
  |E5DRdBmWrUx8XDnblikYGvbQyc1h5c6Rf0VvaEp7W2phTW8dJLOwsI25LTBOwfj9
  |fYTrkl7M6H3hYijvsBW6Fg==
  |-----END ENCRYPTED PRIVATE KEY-----""".stripMargin

  val goodEncryptedKeyPassphrase = "password1"

  val goodConfig: Config.Snowflake = Config.Snowflake(
    url                  = Config.Snowflake.Url("unused", "unused"),
    user                 = "snowplow",
    privateKey           = goodUnencryptedKey,
    privateKeyPassphrase = None,
    role                 = None,
    database             = "snowplow",
    schema               = "snowplow",
    table                = "events",
    channel              = "snowplow",
    jdbcLoginTimeout     = 60.seconds,
    jdbcNetworkTimeout   = 60.seconds,
    jdbcQueryTimeout     = 60.seconds
  )

  val goodConfigWithEncryptedKey = goodConfig.copy(privateKey = goodEncryptedKey, privateKeyPassphrase = Some(goodEncryptedKeyPassphrase))
  val badConfigWithEncryptedKey  = goodConfigWithEncryptedKey.copy(privateKeyPassphrase = Some("nonsese"))
  val badConfigWithMissingPassphrase = goodConfigWithEncryptedKey.copy(privateKeyPassphrase = None)

  class TestMonitoring(val ref: Ref[IO, Vector[Alert]]) extends Monitoring[IO] {
    override def alert(message: Alert): IO[Unit] =
      ref.update(_ :+ message)
  }

  def testMonitoring: IO[TestMonitoring] =
    for {
      ref <- Ref[IO].of(Vector.empty[Alert])
    } yield new TestMonitoring(ref)
}
