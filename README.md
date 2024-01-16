# Snowplow Snowflake Loader

[![Build Status][build-image]][build]
[![Release][release-image]][releases]
[![License][license-image]][license]

## Introduction

This project contains applications required to load Snowplow data into Snowflake with low latency.

Check out [the example config files](./config) for how to configure your loader.

#### Azure

The Azure snowflake loader reads the stream of enriched events from Event Hubs.

Basic usage:
`
```bash
docker run \
  -v /path/to/config.hocon:/var/config.hocon \
  snowplow/snowflake-loader-kafka:0.2.0 \
  --config /var/config.hocon
```

#### GCP

The GCP snowflake loader reads the stream of enriched events from Pubsub.

```bash
docker run \
  -v /path/to/config.hocon:/var/config.hocon \
  snowplow/snowflake-loader-pubsub:0.2.0 \
  --config /var/config.hocon
```

#### AWS

The AWS snowflake loader reads the stream of enriched events from Kinesis.

```bash
docker run \
  -v /path/to/config.hocon:/var/config.hocon \
  snowplow/snowflake-loader-kinesis:0.2.0 \
  --config /var/config.hocon
```

## Find out more

| Technical Docs             | Setup Guide          | Roadmap & Contributing |
|----------------------------|----------------------|------------------------|
| ![i1][techdocs-image]      | ![i2][setup-image]   | ![i3][roadmap-image]   |
| [Technical Docs][techdocs] | [Setup Guide][setup] | [Roadmap][roadmap]     |



## Copyright and License

Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.

Licensed under the [Snowplow Community License](https://docs.snowplow.io/community-license-1.0). _(If you are uncertain how it applies to your use case, check our answers to [frequently asked questions](https://docs.snowplow.io/docs/contributing/community-license-faq/).)_

[techdocs-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/techdocs.png
[setup-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/setup.png
[roadmap-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/roadmap.png
[setup]: https://docs.snowplow.io/docs/getting-started-on-snowplow-open-source/
<!-- TODO: update link when docs site has a snowflake loader page: -->
[techdocs]: https://docs.snowplow.io/docs/pipeline-components-and-applications/loaders-storage-targets/
[roadmap]: https://github.com/snowplow/snowplow/projects/7

[build-image]: https://github.com/snowplow-incubator/snowplow-snowflake-streaming-loader/workflows/CI/badge.svg
[build]: https://github.com/snowplow-incubator/snowplow-snowflake-streaming-loader/actions/workflows/ci.yml

[release-image]: https://img.shields.io/badge/release-0.2.0-blue.svg?style=flat
[releases]: https://github.com/snowplow-incubator/snowplow-snowflake-streaming-loader/releases

[license]: https://docs.snowplow.io/docs/contributing/community-license-faq/
[license-image]: https://img.shields.io/badge/license-Snowplow--Community-blue.svg?style=flat
