/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.sinks

/**
 * A common interface over the external sinks that Snowplow can write to
 *
 * Implementations of this trait are provided by the sinks library (e.g. kinesis, kafka, pubsub)
 */
trait Sink[F[_]] {

  /**
   * Writes a batch of events to the external sink, handling partition keys and message attributes
   */
  def sink(batch: List[Sinkable]): F[Unit]

  /** Writes a batch of events to the sink using an empty partition key and attributes */
  def sinkSimple(batch: List[Array[Byte]]): F[Unit] =
    sink(batch.map(Sinkable(_, None, Map.empty)))

}

object Sink {

  def apply[F[_]](f: List[Sinkable] => F[Unit]): Sink[F] = new Sink[F] {
    def sink(batch: List[Sinkable]): F[Unit] = f(batch)
  }
}
