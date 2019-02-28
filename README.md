# Snowplow Snowflake Loader

## Overview

Snowflake Loader is a project to load Snowplow enriched events into the [Snowflake cloud data warehouse][snowflake]. It consists on two independent applications:

- *Snowplow Snowflake Transformer* - a Spark job responsible for transforming enriched events into a Snowflake-compatible format;
- *Snowplow Snowflake Loader* - a CLI application responsible for loading Snowplow-compatible enriched events into Snowflake DB.

| **[Technical Docs][techdocs]**    | **[Setup Guide][setup]**    | **[Contributing][contributing]**          |
|-----------------------------------|-----------------------------|-------------------------------------------|
| [![i1][techdocs-image]][techdocs] | [![i2][setup-image]][setup] | [![i3][contributing-image]][contributing] |

## Quickstart

Assuming git, **[Vagrant][vagrant-install]** and **[VirtualBox][virtualbox-install]** installed:

```bash
host$ git clone https://github.com/snowplow-product/snowplow-snowflake-loader.git
host$ cd snowplow-snowflake-loader
host$ vagrant up && vagrant ssh
guest$ cd /vagrant
guest$ sbt test
```

## Copyright and License

Snowflake Loader is copyright 2017-2018 Snowplow Analytics Ltd.

Licensed under the **[Apache License, Version 2.0][license]** (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[vagrant-install]: http://docs.vagrantup.com/v2/installation/index.html
[virtualbox-install]: https://www.virtualbox.org/wiki/Downloads

[license-image]: http://img.shields.io/badge/license-Apache--2-blue.svg?style=flat
[license]: http://www.apache.org/licenses/LICENSE-2.0

[travis]: https://travis-ci.org/snowplow/snowplowsnowflaketransformer
[travis-image]: https://travis-ci.org/snowplow/snowplowsnowflaketransformer.png?branch=master

[release-image]: http://img.shields.io/badge/release-0.4.2-blue.svg?style=flat
[releases]: https://github.com/snowplow/snowplowsnowflaketransformer/releases

[snowflake]: https://www.snowflake.com/

[techdocs]: https://github.com/snowplow-incubator/snowplow-snowflake-loader/wiki/
[techdocs-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/techdocs.png

[setup]: https://github.com/snowplow-incubator/snowplow-snowflake-loader/wiki/Setup-Guide
[setup-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/setup.png

[contributing]: https://github.com/snowplow-incubator/snowplow-snowflake-loader/wiki/
[contributing-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/contributing.png
