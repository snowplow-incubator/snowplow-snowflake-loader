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

import scala.concurrent.duration.DurationLong
import java.sql.SQLException

import com.snowplowanalytics.snowplow.snowflake.{Alert, Config, RuntimeService}
import com.snowplowanalytics.snowplow.runtime.{AppHealth, Retrying}

class ChannelProviderSpec extends Specification with CatsEffect {
  import ChannelProviderSpec._

  def is = s2"""
  The channel provider should
    Make no actions if the provider is never used $e1
    Manage channel lifecycle after a channel is opened $e2
    Manage channel lifecycle after an exception using the channel $e3
    Retry opening a channel and send alerts when there is an setup exception opening the channel $e4
    Retry opening a channel if there is a transient exception opening the channel, with limited number of attempts and no monitoring alerts $e5
    Retry setup error according to a single backoff policy when multiple concurrent fibers want to open a channel $e6
    Become healthy after recovering from an earlier setup error $e7
    Become healthy after recovering from an earlier transient error $e8
  """

  def e1 = control.flatMap { c =>
    val io = Channel.provider(c.channelOpener, retriesConfig, c.appHealth).use_

    for {
      _ <- io
      state <- c.state.get
    } yield state should beEmpty
  }

  def e2 = control.flatMap { c =>
    val io = Channel.provider(c.channelOpener, retriesConfig, c.appHealth).use { provider =>
      provider.opened.use_
    }

    val expectedState = Vector(
      Action.OpenedChannel,
      Action.BecameHealthyForSetup,
      Action.BecameHealthy(RuntimeService.Snowflake),
      Action.ClosedChannel
    )

    for {
      _ <- io
      state <- c.state.get
    } yield state should beEqualTo(expectedState)
  }

  def e3 = control.flatMap { c =>
    val io = Channel.provider(c.channelOpener, retriesConfig, c.appHealth).use { provider =>
      provider.opened.use { _ =>
        goBOOM
      }
    }

    val expectedState = Vector(
      Action.OpenedChannel,
      Action.BecameHealthyForSetup,
      Action.BecameHealthy(RuntimeService.Snowflake),
      Action.ClosedChannel
    )

    for {
      _ <- io.voidError
      state <- c.state.get
    } yield state should beEqualTo(expectedState)
  }

  def e4 = control.flatMap { c =>
    // An channel opener that throws an exception when trying to open a channel
    val throwingOpener = new Channel.Opener[IO] {
      def open: IO[Channel.CloseableChannel[IO]] =
        c.channelOpener.open *> raiseForSetupError
    }

    val io = Channel.provider(throwingOpener, retriesConfig, c.appHealth).use { provider =>
      provider.opened.use_
    }

    val expectedState = Vector(
      Action.OpenedChannel,
      Action.SentAlert(0L),
      Action.OpenedChannel,
      Action.SentAlert(30L),
      Action.OpenedChannel,
      Action.SentAlert(90L),
      Action.OpenedChannel,
      Action.SentAlert(210L)
    )

    val test = for {
      fiber <- io.start
      _ <- IO.sleep(4.minutes)
      _ <- fiber.cancel
      state <- c.state.get
    } yield state should beEqualTo(expectedState)

    TestControl.executeEmbed(test)
  }

  def e5 = control.flatMap { c =>
    // An channel opener that throws an exception when trying to open a channel
    val throwingOpener = new Channel.Opener[IO] {
      def open: IO[Channel.CloseableChannel[IO]] =
        c.channelOpener.open *> goBOOM
    }

    val io = Channel.provider(throwingOpener, retriesConfig, c.appHealth).use { provider =>
      provider.opened.use_
    }

    val expectedState = Vector(
      Action.OpenedChannel,
      Action.BecameUnhealthy(RuntimeService.Snowflake),
      Action.OpenedChannel,
      Action.BecameUnhealthy(RuntimeService.Snowflake),
      Action.OpenedChannel,
      Action.BecameUnhealthy(RuntimeService.Snowflake),
      Action.OpenedChannel,
      Action.BecameUnhealthy(RuntimeService.Snowflake),
      Action.OpenedChannel,
      Action.BecameUnhealthy(RuntimeService.Snowflake)
    )

    val test = for {
      _ <- io.voidError
      state <- c.state.get
    } yield state should beEqualTo(expectedState)

    TestControl.executeEmbed(test)
  }

  def e6 = control.flatMap { c =>
    // An opener that throws an exception when trying to open a channel
    val throwingOpener = new Channel.Opener[IO] {
      def open: IO[Channel.CloseableChannel[IO]] =
        c.channelOpener.open *> raiseForSetupError
    }

    // Three concurrent fibers wanting to open the channel:
    val io = Channel.provider(throwingOpener, retriesConfig, c.appHealth).use { provider =>
      Supervisor[IO](await = false).use { supervisor =>
        supervisor.supervise(provider.opened.surround(IO.never)) *>
          supervisor.supervise(provider.opened.surround(IO.never)) *>
          supervisor.supervise(provider.opened.surround(IO.never)) *>
          IO.never
      }
    }

    val expectedState = Vector(
      Action.OpenedChannel,
      Action.SentAlert(0L),
      Action.OpenedChannel,
      Action.SentAlert(30L),
      Action.OpenedChannel,
      Action.SentAlert(90L),
      Action.OpenedChannel,
      Action.SentAlert(210L)
    )

    val test = for {
      fiber <- io.start
      _ <- IO.sleep(4.minutes)
      state <- c.state.get
      _ <- fiber.cancel
    } yield state should beEqualTo(expectedState)

    TestControl.executeEmbed(test)
  }

  def e7 = control.flatMap { c =>
    // An channel opener that throws an exception *once* and is healthy thereafter
    val throwingOnceOpener = Ref[IO].of(false).map { hasThrownException =>
      new Channel.Opener[IO] {
        def open: IO[Channel.CloseableChannel[IO]] =
          hasThrownException.get.flatMap {
            case false =>
              hasThrownException.set(true) *> c.channelOpener.open *> raiseForSetupError
            case true =>
              c.channelOpener.open
          }
      }
    }

    val io = throwingOnceOpener.flatMap { channelOpener =>
      Channel.provider(channelOpener, retriesConfig, c.appHealth).use { provider =>
        provider.opened.use_
      }
    }

    val expectedState = Vector(
      Action.OpenedChannel,
      Action.SentAlert(0L),
      Action.OpenedChannel,
      Action.BecameHealthyForSetup,
      Action.BecameHealthy(RuntimeService.Snowflake),
      Action.ClosedChannel
    )

    val test = for {
      _ <- io
      state <- c.state.get
    } yield state should beEqualTo(expectedState)

    TestControl.executeEmbed(test)
  }

  def e8 = control.flatMap { c =>
    // An channel opener that throws an exception *once* and is healthy thereafter
    val throwingOnceOpener = Ref[IO].of(false).map { hasThrownException =>
      new Channel.Opener[IO] {
        def open: IO[Channel.CloseableChannel[IO]] =
          hasThrownException.get.flatMap {
            case false =>
              hasThrownException.set(true) *> c.channelOpener.open *> goBOOM
            case true =>
              c.channelOpener.open
          }
      }
    }

    val io = throwingOnceOpener.flatMap { channelOpener =>
      Channel.provider(channelOpener, retriesConfig, c.appHealth).use { provider =>
        provider.opened.use_
      }
    }

    val expectedState = Vector(
      Action.OpenedChannel,
      Action.BecameUnhealthy(RuntimeService.Snowflake),
      Action.OpenedChannel,
      Action.BecameHealthyForSetup,
      Action.BecameHealthy(RuntimeService.Snowflake),
      Action.ClosedChannel
    )

    val test = for {
      _ <- io
      state <- c.state.get
    } yield state should beEqualTo(expectedState)

    TestControl.executeEmbed(test)
  }

}

object ChannelProviderSpec {

  sealed trait Action

  object Action {
    case object OpenedChannel extends Action
    case object ClosedChannel extends Action
    case class SentAlert(timeSentSeconds: Long) extends Action
    case class BecameUnhealthy(service: RuntimeService) extends Action
    case class BecameHealthy(service: RuntimeService) extends Action
    case object BecameHealthyForSetup extends Action
  }

  case class Control(
    state: Ref[IO, Vector[Action]],
    channelOpener: Channel.Opener[IO],
    appHealth: AppHealth.Interface[IO, Alert, RuntimeService]
  )

  def retriesConfig = Config.Retries(Retrying.Config.ForSetup(30.seconds), Retrying.Config.ForTransient(1.second, 5))

  def control: IO[Control] =
    for {
      state <- Ref[IO].of(Vector.empty[Action])
    } yield Control(state, testChannelOpener(state), testAppHealth(state))

  private def testAppHealth(state: Ref[IO, Vector[Action]]): AppHealth.Interface[IO, Alert, RuntimeService] =
    new AppHealth.Interface[IO, Alert, RuntimeService] {
      def beHealthyForSetup: IO[Unit] =
        state.update(_ :+ Action.BecameHealthyForSetup)
      def beUnhealthyForSetup(alert: Alert): IO[Unit] =
        for {
          now <- IO.realTime
          _ <- state.update(_ :+ Action.SentAlert(now.toSeconds))
        } yield ()
      def beHealthyForRuntimeService(service: RuntimeService): IO[Unit] =
        state.update(_ :+ Action.BecameHealthy(service))
      def beUnhealthyForRuntimeService(service: RuntimeService): IO[Unit] =
        state.update(_ :+ Action.BecameUnhealthy(service))
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

  // Raise an exception in an IO
  def goBOOM[A]: IO[A] = IO.raiseError(new RuntimeException("boom!")).adaptError { t =>
    t.setStackTrace(Array()) // don't clutter our test logs
    t
  }

  // Raise a known exception that indicates a problem with the warehouse setup
  def raiseForSetupError[A]: IO[A] = IO.raiseError(new SQLException("boom!", "02000", 2003)).adaptError { t =>
    t.setStackTrace(Array()) // don't clutter our test logs
    t
  }

}
