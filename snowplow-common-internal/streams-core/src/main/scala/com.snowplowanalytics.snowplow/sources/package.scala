/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow

import fs2.Pipe
import cats.effect.kernel.Unique

package object sources {

  /**
   * An application that processes a source of events
   *
   * The [[EventProcessor]] is implemented by the specific application (e.g. Enrich or Transformer).
   * Once implemented, we can create a runnable program by pairing it with a [[SourceAndAck]].
   *
   * The [[SourceAndAck]] provides the [[EventProcessor]] with events and tokens. The
   * [[EventProcessor]] must emit the tokens after it has fully processed the events.
   */
  type EventProcessor[F[_]] = Pipe[F, TokenedEvents, Unique.Token]

}
