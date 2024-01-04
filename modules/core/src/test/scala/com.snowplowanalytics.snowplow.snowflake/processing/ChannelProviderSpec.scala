/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package com.snowplowanalytics.snowplow.snowflake.processing

import cats.implicits._
import cats.effect.{IO, Ref}
import cats.effect.std.Supervisor
import org.specs2.Specification
import cats.effect.testing.specs2.CatsEffect
import cats.effect.testkit.TestControl

import scala.concurrent.duration.{DurationLong, FiniteDuration}
import com.snowplowanalytics.snowplow.snowflake.{Alert, AppHealth, Config, Monitoring}
import com.snowplowanalytics.snowplow.runtime.HealthProbe
import com.snowplowanalytics.snowplow.snowflake.AppHealth.Service.{BadSink, Snowflake}
import com.snowplowanalytics.snowplow.sources.{EventProcessingConfig, EventProcessor, SourceAndAck}

class ChannelProviderSpec extends Specification with CatsEffect {
  import ChannelProviderSpec._

  def is = s2"""
  The channel provider should
    Make no actions if the provider is never used $e1
    Manage channel lifecycle after a channel is opened $e2
    Manage channel lifecycle after an exception using the channel $e3
    Retry opening a channel when there is an exception opening the channel $e4
    Retry according to a single backoff policy when multiple concurrent fibers want to open a channel $e5
    Become healthy after recovering from an earlier failure $e6
  """

  def e1 = control.flatMap { c =>
    val io = Channel.provider(c.channelOpener, retriesConfig, c.appHealth, c.monitoring).use_

    for {
      _ <- io
      state <- c.state.get
      health <- c.appHealth.status()
    } yield List(
      state should beEqualTo(Vector()),
      health should beHealthy
    ).reduce(_ and _)
  }

  def e2 = control.flatMap { c =>
    val io = Channel.provider(c.channelOpener, retriesConfig, c.appHealth, c.monitoring).use { provider =>
      provider.opened.use_
    }

    val expectedState = Vector(
      Action.OpenedChannel,
      Action.ClosedChannel
    )

    for {
      _ <- io
      state <- c.state.get
      health <- c.appHealth.status()
    } yield List(
      state should beEqualTo(expectedState),
      health should beHealthy
    ).reduce(_ and _)
  }

  def e3 = control.flatMap { c =>
    val io = Channel.provider(c.channelOpener, retriesConfig, c.appHealth, c.monitoring).use { provider =>
      provider.opened.use { _ =>
        goBOOM
      }
    }

    val expectedState = Vector(
      Action.OpenedChannel,
      Action.ClosedChannel
    )

    for {
      _ <- io.voidError
      state <- c.state.get
      health <- c.appHealth.status()
    } yield List(
      state should beEqualTo(expectedState),
      health should beHealthy
    ).reduce(_ and _)
  }

  def e4 = control.flatMap { c =>
    // An channel opener that throws an exception when trying to open a channel
    val throwingOpener = new Channel.Opener[IO] {
      def open: IO[Channel.CloseableChannel[IO]] = goBOOM
    }

    val io = Channel.provider(throwingOpener, retriesConfig, c.appHealth, c.monitoring).use { provider =>
      provider.opened.use_
    }

    val expectedState = Vector(
      Action.SentAlert(0L),
      Action.SentAlert(30L),
      Action.SentAlert(90L),
      Action.SentAlert(210L)
    )

    val test = for {
      fiber <- io.start
      _ <- IO.sleep(4.minutes)
      _ <- fiber.cancel
      state <- c.state.get
      health <- c.appHealth.status()
    } yield List(
      state should beEqualTo(expectedState),
      health should beUnhealthy
    ).reduce(_ and _)

    TestControl.executeEmbed(test)
  }

  def e5 = control.flatMap { c =>
    // An opener that throws an exception when trying to open a channel
    val throwingOpener = new Channel.Opener[IO] {
      def open: IO[Channel.CloseableChannel[IO]] = goBOOM
    }

    // Three concurrent fibers wanting to open the channel:
    val io = Channel.provider(throwingOpener, retriesConfig, c.appHealth, c.monitoring).use { provider =>
      Supervisor[IO](await = false).use { supervisor =>
        supervisor.supervise(provider.opened.surround(IO.never)) *>
          supervisor.supervise(provider.opened.surround(IO.never)) *>
          supervisor.supervise(provider.opened.surround(IO.never)) *>
          IO.never
      }
    }

    val expectedState = Vector(
      Action.SentAlert(0L),
      Action.SentAlert(30L),
      Action.SentAlert(90L),
      Action.SentAlert(210L)
    )

    val test = for {
      fiber <- io.start
      _ <- IO.sleep(4.minutes)
      state <- c.state.get
      health <- c.appHealth.status()
      _ <- fiber.cancel
    } yield List(
      state should beEqualTo(expectedState),
      health should beUnhealthy
    ).reduce(_ and _)
    TestControl.executeEmbed(test)
  }

  def e6 = control.flatMap { c =>
    // An channel opener that throws an exception *once* and is healthy thereafter
    val throwingOnceOpener = Ref[IO].of(false).map { hasThrownException =>
      new Channel.Opener[IO] {
        def open: IO[Channel.CloseableChannel[IO]] =
          hasThrownException.get.flatMap {
            case false =>
              hasThrownException.set(true) *> goBOOM
            case true =>
              c.channelOpener.open
          }
      }
    }

    val io = throwingOnceOpener.flatMap { channelOpener =>
      Channel.provider(channelOpener, retriesConfig, c.appHealth, c.monitoring).use { provider =>
        provider.opened.use_
      }
    }

    val expectedState = Vector(
      Action.SentAlert(0L),
      Action.OpenedChannel,
      Action.ClosedChannel
    )

    val test = for {
      _ <- io
      state <- c.state.get
      health <- c.appHealth.status()
    } yield List(
      state should beEqualTo(expectedState),
      health should beHealthy
    ).reduce(_ and _)
    TestControl.executeEmbed(test)
  }

  /** Convenience matchers for health probe * */

  def beHealthy: org.specs2.matcher.Matcher[HealthProbe.Status] = { (status: HealthProbe.Status) =>
    val result = status match {
      case HealthProbe.Healthy      => true
      case HealthProbe.Unhealthy(_) => false
    }
    (result, s"$status is not healthy")
  }

  def beUnhealthy: org.specs2.matcher.Matcher[HealthProbe.Status] = { (status: HealthProbe.Status) =>
    val result = status match {
      case HealthProbe.Healthy      => false
      case HealthProbe.Unhealthy(_) => true
    }
    (result, s"$status is not unhealthy")
  }
}

object ChannelProviderSpec {

  sealed trait Action

  object Action {
    case object OpenedChannel extends Action
    case object ClosedChannel extends Action
    case class SentAlert(timeSentSeconds: Long) extends Action
  }

  case class Control(
    state: Ref[IO, Vector[Action]],
    channelOpener: Channel.Opener[IO],
    appHealth: AppHealth[IO],
    monitoring: Monitoring[IO]
  )

  def retriesConfig = Config.Retries(backoff = 30.seconds)

  def control: IO[Control] =
    for {
      state <- Ref[IO].of(Vector.empty[Action])
      appHealth <- testAppHealth()
    } yield Control(state, testChannelOpener(state), appHealth, testMonitoring(state))

  private def testAppHealth(): IO[AppHealth[IO]] = {
    val everythingHealthy: Map[AppHealth.Service, Boolean] = Map(Snowflake -> true, BadSink -> true)
    val healthySource = new SourceAndAck[IO] {
      override def stream(config: EventProcessingConfig, processor: EventProcessor[IO]): fs2.Stream[IO, Nothing] =
        fs2.Stream.empty

      override def isHealthy(maxAllowedProcessingLatency: FiniteDuration): IO[SourceAndAck.HealthStatus] =
        IO(SourceAndAck.Healthy)
    }
    AppHealth.init(10.seconds, healthySource, everythingHealthy)
  }

  private def testChannelOpener(state: Ref[IO, Vector[Action]]): Channel.Opener[IO] =
    new Channel.Opener[IO] {
      def open: IO[Channel.CloseableChannel[IO]] =
        state.update(_ :+ Action.OpenedChannel).as(testCloseableChannel(state))
    }

  private def testCloseableChannel(state: Ref[IO, Vector[Action]]): Channel.CloseableChannel[IO] = new Channel.CloseableChannel[IO] {
    def write(rows: Iterable[Map[String, AnyRef]]): IO[Channel.WriteResult] = IO.pure(Channel.WriteResult.WriteFailures(Nil))

    def close: IO[Unit] = state.update(_ :+ Action.ClosedChannel)
  }

  private def testMonitoring(state: Ref[IO, Vector[Action]]): Monitoring[IO] = new Monitoring[IO] {
    def alert(message: Alert): IO[Unit] =
      for {
        now <- IO.realTime
        _ <- state.update(_ :+ Action.SentAlert(now.toSeconds))
      } yield ()
  }

  // Raise an exception in an IO
  def goBOOM[A]: IO[A] = IO.raiseError(new RuntimeException("boom!")).adaptError { t =>
    t.setStackTrace(Array()) // don't clutter our test logs
    t
  }

}
