package com.snowplowanalytics.snowplow.snowflake

import com.snowplowanalytics.snowplow.snowflake.model.BatchAfterTransform

trait DestinationWriter[F[_]] {

  def writeBatch(batch: BatchAfterTransform): F[BatchAfterTransform]

}
